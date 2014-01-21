/*
 * exact_logical_locality_index.c
 *
 *  Created on: Nov 19, 2013
 *      Author: fumin
 */

#include "index.h"
#include "global_fingerprint_index.h"
#include "segmentstore.h"
#include "aio_segmentstore.h"
#include "../tools/lru_cache.h"
#include "exact_similarity_index.h"

static struct lruCache* segment_recipe_cache;

void init_exact_similarity_index() {

	db_init();

	if (destor.index_segment_selection_method[0] != INDEX_SEGMENT_SELECT_LATEST) {
		destor_log(DESTOR_NOTICE, "Change selection method to LATEST!");
		destor.index_segment_selection_method[0] = INDEX_SEGMENT_SELECT_LATEST;
	}
	init_segment_management();

	segment_recipe_cache = new_lru_cache(destor.index_segment_cache_size,
			free_segment_recipe, lookup_fingerprint_in_segment_recipe);

}

void close_exact_similarity_index() {

	db_close();

	close_segment_management();

	free_lru_cache(segment_recipe_cache);
}

void exact_similarity_index_lookup(struct segment* s) {

	/* BUffered segment, only indexElem without data. */
	struct segment* bs = new_segment();

	int len = g_queue_get_length(s->chunks), i;

	for (i = 0; i < len; ++i) {
		struct chunk* c = g_queue_peek_nth(s->chunks, i);

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
			continue;

		GQueue *tq = g_hash_table_lookup(index_buffer.table, &c->fp);
		if (tq) {
			struct indexElem *be = g_queue_peek_head(tq);
			c->id = be->id;
			SET_CHUNK(c, CHUNK_DUPLICATE);
		} else {
			tq = g_queue_new();
		}

		if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			struct segmentRecipe* sr = lru_cache_lookup(segment_recipe_cache,
					&c->fp);
			if (sr) {
				/* Find it */
				SET_CHUNK(c, CHUNK_DUPLICATE);
				struct indexElem* e = g_hash_table_lookup(sr->index, &c->fp);
				assert(e);
				c->id = e->id;
			}
		}

		if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			segmentid id = db_lookup_fingerprint(&c->fp);
			if (id != TEMPORARY_ID) {
				assert(
						!lru_cache_hits(segment_recipe_cache, &id,
								segment_recipe_check_id));
				jcr.index_lookup_io++;
				GQueue* segments = prefetch_segments(id,
						destor.index_segment_prefech);
				struct segmentRecipe* sr;
				while ((sr = g_queue_pop_tail(segments))) {
					/* From tail to head */
					if (!lru_cache_hits(segment_recipe_cache, &sr->id,
							segment_recipe_check_id)) {
						lru_cache_insert(segment_recipe_cache, sr, NULL, NULL);
					} else {
						/* Already in cache */
						free_segment_recipe(sr);
					}
				}
				g_queue_free(segments);

				sr = lru_cache_lookup(segment_recipe_cache,
						&c->fp);
				assert(sr);
				/* Find it */
				SET_CHUNK(c, CHUNK_DUPLICATE);
				struct indexElem* e = g_hash_table_lookup(sr->index, &c->fp);
				assert(e);
				c->id = e->id;

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

	bs->features = s->features;
	s->features = NULL;
	g_queue_push_tail(index_buffer.segment_queue, bs);

}

containerid exact_similarity_index_update(fingerprint *fp, containerid from,
		containerid to) {
	static int n = 0;
	static struct segmentRecipe *srbuf;

	if (srbuf == NULL)
		srbuf = new_segment_recipe();

	containerid final_id = TEMPORARY_ID;

	struct segment *bs = g_queue_peek_head(index_buffer.segment_queue); // current segment

	struct indexElem* e = g_queue_peek_nth(bs->chunks, n++); // current chunk

	assert(to >= from);
	assert(e->id >= from);
	assert(g_fingerprint_equal(fp, &e->fp));

	if (from < e->id) {
		/* to is meaningless. */
		final_id = e->id;
	} else {

		if (from != to) {

			GQueue *tq = g_hash_table_lookup(index_buffer.table, &e->fp);
			assert(tq);

			int len = g_queue_get_length(tq), i;
			for (i = 0; i < len; i++) {
				struct indexElem* ue = g_queue_peek_nth(tq, i);
				ue->id = to;
			}

			if (!g_hash_table_contains(srbuf->features, &e->fp)) {
				fingerprint* f = (fingerprint*) malloc(sizeof(fingerprint));
				memcpy(f, &e->fp, sizeof(fingerprint));
				g_hash_table_insert(srbuf->features, f, f);
			}
		} else {
			/* a normal redundant chunk */
			assert(to!=TEMPORARY_ID);
		}
	}

	/* Add the elem into the segmentRecipe buffer */
	struct indexElem * be = (struct indexElem*) malloc(
			sizeof(struct indexElem));
	be->id = (final_id == TEMPORARY_ID) ? to : final_id;
	memcpy(&be->fp, fp, sizeof(fingerprint));
	g_hash_table_replace(srbuf->index, &be->fp, be);

	if (n == g_queue_get_length(bs->chunks)) {
		/*
		 * Current segment is finished.
		 * We remove it from buffer.
		 * */
		bs = g_queue_pop_head(index_buffer.segment_queue);

		/* Clear the segment */
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

		if (bs->features) {
			GHashTableIter iter;
			gpointer key, value;
			g_hash_table_iter_init(&iter, bs->features);
			while (g_hash_table_iter_next(&iter, &key, &value)) {
				if (!g_hash_table_contains(srbuf->features,
						(fingerprint*) key)) {
					fingerprint* f = (fingerprint*) malloc(sizeof(fingerprint));
					memcpy(f, (fingerprint*) key, sizeof(fingerprint));
					g_hash_table_insert(srbuf->features, f, f);
				}
			}
		}

		free_segment(bs, free);

		jcr.index_update_io++;
		srbuf = update_segment(srbuf);

		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, srbuf->features);
		while (g_hash_table_iter_next(&iter, &key, &value)) {
			jcr.index_update_io++;
			db_insert_fingerprint((fingerprint*) value, srbuf->id);
		}

		free_segment_recipe(srbuf);
		srbuf = NULL;
		n = 0;
	}

	return final_id;
}
