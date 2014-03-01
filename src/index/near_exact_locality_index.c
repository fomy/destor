/*
 * near_exact_locality_index.c
 *
 *  Created on: Nov 15, 2013
 *      Author: fumin
 */

#include "index.h"
#include "near_exact_locality_index.h"
#include "feature_index.h"
#include "../tools/lru_cache.h"
#include "../storage/containerstore.h"

static struct lruCache* container_meta_cache = NULL;

void init_near_exact_locality_index() {

	init_feature_index();

	if (destor.index_segment_algorithm[0] != INDEX_SEGMENT_FIXED) {
		destor_log(DESTOR_NOTICE, "Change segment method to 1024-fixed!");
		destor.index_segment_algorithm[0] = INDEX_SEGMENT_FIXED;
		destor.index_segment_algorithm[1] = 1024;
	}

	if (destor.index_feature_method[1] > 1) {
		/*
		 * If destor.index_feature_method[1] == 1,
		 * all fingerprints are reserved in memory.
		 * Thus container prefetching is unnecessary.
		 */
		container_meta_cache = new_lru_cache(destor.index_container_cache_size,
				free_container_meta, lookup_fingerprint_in_container_meta);

	}
}

void close_near_exact_locality_index() {
	if (index_buffer.cid != TEMPORARY_ID) {
		GHashTable *features = featuring(NULL, 1);

		GHashTableIter iter;
		gpointer feature, value;
		g_hash_table_iter_init(&iter, features);
		while (g_hash_table_iter_next(&iter, &feature, &value))
			feature_index_update((fingerprint*) feature, index_buffer.cid);

		g_hash_table_destroy(features);
	}

	close_feature_index();

	if (container_meta_cache)
		free_lru_cache(container_meta_cache);

}

void near_exact_locality_index_lookup(struct segment* s) {

	/*
	 * In the category,
	 * the notion of segment is only for batch process,
	 * not for similarity detection.
	 */
	/* bufferred segment */
	struct segment *bs = new_segment();

	int len = g_queue_get_length(s->chunks), i;

	/* Traverse the segment */
	for (i = 0; i < len; ++i) {
		struct chunk* c = g_queue_peek_nth(s->chunks, i);

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
			continue;

		/* Examine the buffered features that have not been updated to index. */
		if (index_buffer.feature_buffer
				&& g_queue_find_custom(index_buffer.feature_buffer, &c->fp,
						g_chunk_cmp)) {
			assert(index_buffer.cid != TEMPORARY_ID);
			c->id = index_buffer.cid;
			SET_CHUNK(c, CHUNK_DUPLICATE);
		}

		/* Examine the buffered fingerprints */
		GQueue *tq = g_hash_table_lookup(index_buffer.table, &c->fp);
		if (!tq) {
			tq = g_queue_new();
		} else if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			struct indexElem *be = g_queue_peek_head(tq);
			c->id = be->id;
			SET_CHUNK(c, CHUNK_DUPLICATE);
		}

		/* Examnine the cached fingerprints */
		if (!CHECK_CHUNK(c, CHUNK_DUPLICATE) && container_meta_cache) {
			struct containerMeta* cm = lru_cache_lookup(container_meta_cache,
					c->fp);
			if (cm) {
				/* Find it */
				SET_CHUNK(c, CHUNK_DUPLICATE);
				c->id = cm->id;
			}
		}

		/* Lookup the feature index */
		if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			containerid id = feature_index_lookup_for_latest(&c->fp);

			if (id != TEMPORARY_ID) {
				/* Find it */
				SET_CHUNK(c, CHUNK_DUPLICATE);
				c->id = id;

				if (container_meta_cache) {
					struct containerMeta * cm = retrieve_container_meta_by_id(
							c->id);
					if (cm) {
						jcr.index_lookup_io++;
						lru_cache_insert(container_meta_cache, cm, NULL, NULL);
					} else
						destor_log(DESTOR_NOTICE,
								"The container %lld has not been written!",
								c->id);
				}
			}
		}

		struct indexElem *ne = (struct indexElem*) malloc(
				sizeof(struct indexElem));
		ne->id = c->id;
		memcpy(&ne->fp, &c->fp, sizeof(fingerprint));

		g_queue_push_tail(bs->chunks, ne);
		bs->chunk_num++;

		g_queue_push_tail(tq, ne);
		g_hash_table_replace(index_buffer.table, &ne->fp, tq);

		index_buffer.num++;
	}

	g_queue_push_tail(index_buffer.segment_queue, bs);
}

containerid near_exact_locality_index_update(fingerprint *fp, containerid from,
		containerid to) {
	static int n = 0;

	containerid final_id = TEMPORARY_ID;

	struct segment* bs = g_queue_peek_head(index_buffer.segment_queue); // current segment

	struct indexElem* e = g_queue_peek_nth(bs->chunks, n++); // current chunk

	assert(to >= from);
	assert(e->id >= from);
	assert(g_fingerprint_equal(fp, &e->fp));

	if (from < e->id) {
		/* to is meaningless.
		 * An identical chunk has been written very recently. */
		final_id = e->id;
	} else {

		if (from != to) {
			/* It is written. */

			if (index_buffer.cid != TEMPORARY_ID && index_buffer.cid != to) {
				/* Another container */
				GHashTable *features = featuring(index_buffer.feature_buffer,
						0);

				GHashTableIter iter;
				gpointer key, value;
				g_hash_table_iter_init(&iter, features);
				while (g_hash_table_iter_next(&iter, &key, &value))
					feature_index_update((fingerprint*) key, index_buffer.cid);

				g_hash_table_destroy(features);

				g_queue_free_full(index_buffer.feature_buffer, free_chunk);
				index_buffer.feature_buffer = g_queue_new();
			}

			index_buffer.cid = to;
			struct chunk* candidate = new_chunk(0);
			memcpy(&candidate->fp, fp, sizeof(fingerprint));
			g_queue_push_tail(index_buffer.feature_buffer, candidate);

			GQueue *tq = g_hash_table_lookup(index_buffer.table, &e->fp);
			assert(tq);

			int len = g_queue_get_length(tq), i;
			for (i = 0; i < len; i++) {
				struct indexElem* ue = g_queue_peek_nth(tq, i);
				ue->id = to;
			}
		} else {
			/* a normal redundant chunk */
			assert(to!=TEMPORARY_ID);
		}
	}

	if (n == g_queue_get_length(bs->chunks)) {
		/*
		 * Current segment is finished.
		 * We remove it from buffer.
		 * */
		bs = g_queue_pop_head(index_buffer.segment_queue);

		struct indexElem* ee = g_queue_pop_head(bs->chunks);
		do {
			GQueue *tq = g_hash_table_lookup(index_buffer.table, &ee->fp);
			assert(g_queue_peek_head(tq) == ee);
			g_queue_pop_head(tq);
			if (g_queue_get_length(tq) == 0) {
				g_hash_table_remove(index_buffer.table, &ee->fp);
				g_queue_free(tq);
			}
			free(ee);
			index_buffer.num--;
		} while ((ee = g_queue_pop_head(bs->chunks)));

		free_segment(bs, free);
		n = 0;
	}

	return final_id;
}
