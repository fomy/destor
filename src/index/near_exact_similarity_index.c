/*
 * near_exact_similarity_index.c
 *
 *  Created on: Nov 19, 2013
 *      Author: fumin
 */

#include "near_exact_similarity_index.h"
#include "index.h"
#include "feature_index.h"
#include "../tools/lru_cache.h"
#include "segment_management.h"
#include "aio_segment_management.h"

static struct lruCache* segment_recipe_cache;
/* For ALL select method. */
static GQueue *segment_buffer;

void init_near_exact_similarity_index() {
	init_feature_index();

	if (destor.index_segment_selection_method[0] == INDEX_SEGMENT_SELECT_ALL) {
		init_aio_segment_management();
	} else if (destor.index_segment_selection_method[0]
			== INDEX_SEGMENT_SELECT_LATEST
			|| destor.index_segment_selection_method[0]
					== INDEX_SEGMENT_SELECT_TOP) {
		init_segment_management();
	}

	segment_recipe_cache = new_lru_cache(destor.index_segment_cache_size,
			free_segment_recipe, lookup_fingerprint_in_segment_recipe);

	if (destor.index_segment_selection_method[0] == INDEX_SEGMENT_SELECT_ALL) {
		segment_buffer = g_queue_new();
	}
}

void close_near_exact_similarity_index() {
	close_feature_index();

	if (destor.index_segment_selection_method[0] == INDEX_SEGMENT_SELECT_ALL) {
		close_aio_segment_management();
	} else if (destor.index_segment_selection_method[0]
			== INDEX_SEGMENT_SELECT_LATEST
			|| destor.index_segment_selection_method[0]
					== INDEX_SEGMENT_SELECT_TOP) {
		close_segment_management();
	}

	free_lru_cache(segment_recipe_cache);

}

/*
 * Enable/Disable segment prefetching.
 */
static void latest_segment_select(GHashTable* features) {

	segmentid latest = TEMPORARY_ID;

	GHashTableIter iter;
	fingerprint *feature, *value;
	g_hash_table_iter_init(&iter, &feature, &value);
	while (g_hash_table_iter_next(&iter, &feature, &value)) {
		segmentid id = feature_index_lookup_for_latest(feature);
		if (id > latest)
			latest = id;
	}

	if (latest != TEMPORARY_ID) {
		if (!lru_cache_hits(segment_recipe_cache, &latest,
				segment_recipe_check_id)) {
			struct segmentRecipe * sr = retrieve_segment(latest);
			assert(sr);
			lru_cache_insert(segment_recipe_cache, sr, NULL, NULL);
		}
	}

}

/*
 * Larger one comes before smaller one.
 * Descending order.
 */
static gint g_segment_feature_size_cmp(struct segmentRecipe* a,
		struct segmentRecipe* b, gpointer user_data) {
	return g_hash_table_size(b->features) - g_hash_table_size(a->features);
}

/*
 * Remove the features that are common with top from target.
 */
static void features_trim(struct segmentRecipe *target,
		struct segmentRecipe *top) {
	GHashTableIter iter;
	fingerprint *key, *value;
	g_hash_table_iter_init(&iter, top->features);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		g_hash_table_remove(target->features, key);
	}
}

static void top_segment_select(GHashTable* features) {
	GHashTable *table = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL,
			free_segment_recipe);

	GHashTableIter iter;
	fingerprint *key, *value;
	g_hash_table_iter_init(&iter, features);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		GQueue *ids = feature_index_lookup(key);
		if (ids) {
			int num = g_queue_get_length(ids), i;
			for (i = 0; i < num; i++) {
				segmentid *sid = g_queue_peak_nth(ids, i);
				struct segmentRecipe* sr = g_hash_table_lookup(table, sid);
				if (!sr) {
					sr = new_segment_recipe();
					sr->id = *sid;
					g_hash_table_insert(table, &sr->id, sr);
				}
				fingerprint *feature = (fingerprint*) malloc(
						sizeof(fingerprint));
				memcpy(feature, key, sizeof(fingerprint));
				assert(!g_hash_table_contains(sr->features, feature));
				g_hash_table_insert(sr->features, feature, feature);
			}
		}
	}

	if (g_hash_table_size(table) != 0) {
		GQueue *segments = g_queue_new();

		/* Sort */
		GSequence *seq = g_sequence_new(NULL);

		GHashTableIter iter;
		segmentid *key;
		struct segmentRecipe* value;
		g_hash_table_iter_init(&iter, table);
		while (g_hash_table_iter_next(&iter, &key, &value)) {
			g_sequence_insert_sorted(seq, value, g_segment_feature_size_cmp,
			NULL);
		}

		int num =
				g_sequence_get_length(seq)
						> destor.index_segment_selection_method[1] ?
						destor.index_segment_selection_method[1] :
						g_sequence_get_length(seq), i;
		for (i = 0; i < num; i++) {
			/* Get the top segment */
			struct segmentRecipe *top = g_sequence_get(
					g_sequence_get_begin_iter(seq));

			if (!lru_cache_hits(segment_recipe_cache, &top->id,
					segment_recipe_check_id)) {
				/* If it doesn't exist in the cahce. */
				struct segmentRecipe* sr = retrieve_segment(top->id);

				g_queue_push_tail(segments, sr);

			}

			g_sequence_remove(g_sequence_get_begin_iter(seq));
			g_sequence_foreach(seq, features_trim, top);
			g_sequence_sort(seq, g_segment_feature_size_cmp, NULL);
		}
		g_sequence_free(seq);

		struct segmentRecipe* sr = g_queue_pop_tail(segments);
		do {
			lru_cache_insert(segment_recipe_cache, sr, NULL, NULL);
		} while ((sr = g_queue_pop_tail(segments)));

		g_queue_free(segments);
	}

	g_hash_table_destroy(table);
}

static struct segmentRecipe* segment_absorb(struct segmentRecipe* base,
		struct segmentRecipe* delta) {
	base->id = TEMPORARY_ID;

	GHashTableIter iter;
	fingerprint *key, *value;
	g_hash_table_iter_init(&iter, delta->features);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		assert(!g_hash_table_contains(base->features, key));
		fingerprint *feature = (fingerprint*) malloc(sizeof(fingerprint));
		memcpy(feature, key, sizeof(fingerprint));
		g_hash_table_insert(base->features, feature, feature);
	}

	struct indexElem* ie;
	g_hash_table_iter_init(&iter, delta->table);
	while (g_hash_table_iter_next(&iter, &key, &ie)) {
		struct indexElem* base_elem = g_hash_table_lookup(base->table, key);
		if (!base_elem) {
			struct indexElem* ne = (struct indexElem*) malloc(
					sizeof(struct indexElem));
			memcpy(ne, ie, sizeof(struct indexElem));
			g_hash_table_insert(base->table, &ne->fp, ne);
		} else {
			/* Select the newer id. More discussions are required. */
			base_elem->id = base_elem->id > ie->id ? base_elem->id : ie->id;
		}
	}

	return base;
}

static void all_segment_select(GHashTable* features) {
	GHashTable *segments = g_hash_table_new_full(g_int64_hash, g_int64_equal,
	NULL, free_segment_recipe);

	GHashTableIter iter;
	fingerprint *feature, *value;
	g_hash_table_iter_init(&iter, &feature, &value);
	while (g_hash_table_iter_next(&iter, &feature, &value)) {
		segmentid id = feature_index_lookup_for_latest(feature);
		if (id != TEMPORARY_ID && g_hash_table_contains(segments, &id)) {
			struct segmentRecipe* sr = retrieve_segment_all_in_one(id);
			g_hash_table_insert(segments, &sr->id, sr);
		}
	}

	if (g_hash_table_size(segments) > 0) {
		struct segmentRecipe* selected = new_segment_recipe();

		GHashTableIter iter;
		segmentid *sid;
		struct segmentRecipe *sr;
		g_hash_table_iter_init(&iter, segments);
		while (g_hash_table_iter_next(&iter, &sid, &sr)) {
			selected = segment_absorb(selected, sr);
		}

		g_queue_push_tail(segment_recipe_dup(selected));
		lru_cache_insert(segment_recipe_cache, selected, NULL, NULL);
	}

	g_hash_table_destroy(segments);
}

void near_exact_similarity_index_lookup(struct segment* s) {
	/* Load similar segments */
	if (destor.index_segment_selection_method[0] == INDEX_SEGMENT_SELECT_ALL) {
		all_segment_select(s->features);
	} else if (destor.index_segment_selection_method[0]
			== INDEX_SEGMENT_SELECT_TOP) {
		top_segment_select(s->features);
	} else if (destor.index_segment_selection_method[0]
			== INDEX_SEGMENT_SELECT_LATEST) {
		latest_segment_select(s->features);
	} else {
		fprintf(stderr, "Invalid segment selection method.\n");
		exit(1);
	}

	/* Dedup the segment */
	struct segment* bs = new_segment();

	int len = g_queue_get_length(s->chunks), i;

	for (i = 0; i < len; ++i) {
		struct chunk* c = g_queue_peek_nth(s->chunks, i);

		if (c->size < 0)
			continue;

		GQueue *tq = g_hash_table_lookup(index_buffer.table, &c->fp);
		if (tq) {
			struct indexElem *be = g_queue_peak_head(tq);
			c->id = be->id;
			c->flag |= CHUNK_DUPLICATE;
		} else {
			tq = g_queue_new();
		}

		if (CHECK_CHUNK_UNIQUE(c) && segment_recipe_cache) {
			struct segmentIndex* si = lru_cache_lookup(segment_recipe_cache,
					&c->fp);
			if (si) {
				/* Find it */
				struct indexElem *ie = g_hash_table_lookup(si->table, c->fp);
				assert(ie);
				c->flag |= CHUNK_DUPLICATE;
				c->id = ie->id;
			}
		}

		struct indexElem *ne = (struct indexElem*) malloc(
				sizeof(struct indexElem));
		ne->id = c->id;
		memcpy(&ne->fp, &c->fp, sizeof(fingerprint));

		g_queue_push_tail(bs->chunks, ne);
		g_queue_push_tail(tq, ne);
		g_hash_table_replace(index_buffer.table, &ne->fp, tq);

	}

	g_queue_push_tail(index_buffer.segment_queue, bs);
}

containerid near_exact_similarity_index_update(fingerprint fp, containerid from,
		containerid to) {
	static int n = 0;
	static struct segmentRecipe *srbuf;

	if (srbuf == NULL)
		srbuf = new_segment_recipe();

	containerid final_id = TEMPORARY_ID;

	struct segment *bs = g_queue_peak_head(index_buffer.segment_queue); // current segment

	struct indexElem* e = g_queue_peak_nth(bs->chunks, n++); // current chunk

	assert(from >= to);
	assert(e->id >= from);
	assert(g_fingerprint_equal(&fp, &e->fp));
	assert(
			g_queue_peak_head(g_hash_table_lookup(index_buffer.table, &fp))
					== e);

	if (from < e->id) {
		/* to is meaningless. */
		final_id = e->id;
	} else {

		if (from != to) {

			GQueue *tq = g_hash_table_lookup(index_buffer.table, &e->fp);
			assert(tq);

			int len = g_queue_get_length(tq);
			int i;
			for (i = 0; i < len; i++) {
				struct indexElem* ue = g_queue_peak_nth(tq, i);
				ue->id = to;
			}
		} else {
			/* a normal redundant chunk */
		}
	}

	struct indexElem * be = (struct indexElem*) malloc(
			sizeof(struct indexElem));
	be->id = final_id == TEMPORARY_ID ? to : final_id;
	memcpy(&be->fp, &fp, sizeof(fingerprint));
	g_hash_table_replace(srbuf->table, &be->fp, be);

	if (n == g_queue_get_length(bs->chunks)) {
		/*
		 * Current segment is finished.
		 * We remove it from buffer.
		 * */
		bs = g_queue_pop_head(index_buffer.segment_queue);

		struct indexElem* ee = g_queue_pop_head(bs->chunks);
		do {
			GQueue *tq = g_hash_table_lookup(index_buffer.table, &ee->fp);
			assert(g_queue_peak_head(tq) == ee);
			g_queue_pop_head(tq);
			if (g_queue_get_length(tq) == 0) {
				/* tp is freed by hash table automatically. */
				g_hash_table_remove(index_buffer.table, &ee->fp);
			}
			free(ee);
		} while ((ee = g_queue_pop_head(bs->chunks)));

		if (bs->features) {
			GHashTableIter iter;
			fingerprint *feature, *value;
			g_hash_table_iter_init(&iter, bs->features);
			while (g_hash_table_inter_next(&iter, &feature, &value)) {
				if (!g_hash_table_contains(srbuf->features, feature)) {
					fingerprint* f = (fingerprint*) malloc(sizeof(fingerprint));
					memcpy(f, feature, sizeof(fingerprint));
					g_hash_table_insert(srbuf->features, f, f);
				}
			}
		}

		free_segment(bs, free);

		if (destor.index_segment_selection_method[0] == INDEX_SEGMENT_SELECT_ALL) {
			struct segmentRecipe* base = g_queue_pop_head(segment_buffer);
			/* New fingerprints coverwrite old ones. */
			base = segment_absorb(base, srbuf);
			free_segment_recipe(srbuf);
			srbuf = base;
			srbuf = update_segment_all_in_one(srbuf);
		} else if (destor.index_segment_selection_method[0]
				== INDEX_SEGMENT_SELECT_TOP) {
			srbuf = update_segment(srbuf);
		} else if (destor.index_segment_selection_method[0]
				== INDEX_SEGMENT_SELECT_LATEST) {
			srbuf = update_segment(srbuf);
		} else {
			fprintf(stderr, "Invalid segment selection method.\n");
			exit(1);
		}

		GHashTableIter iter;
		gpointer key;
		struct indexElem* e;
		g_hash_table_iter_init(&iter, srbuf->features);
		while (g_hash_table_iter_next(&iter, &key, &e)) {
			feature_index_update(&e->fp, srbuf->id);
		}

		free_segment_recipe(srbuf);
		srbuf = NULL;
		n = 0;
	}

	return final_id;
}
