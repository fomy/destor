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
#include "segmentstore.h"
#include "aio_segmentstore.h"

static struct lruCache* segment_recipe_cache;
/* For ALL select method. */
static GQueue *segment_buffer;
static double read_segment_time;

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
			unref_segment_recipe, lookup_fingerprint_in_segment_recipe);

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

	VERBOSE("Read segment time: %.3fs!", read_segment_time / 1000000);
}

/*
 * Enable/Disable segment prefetching.
 */
static void latest_segment_select(GHashTable* features) {

	segmentid latest = TEMPORARY_ID;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, features);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		segmentid id = feature_index_lookup_for_latest((fingerprint*) key);
		if (id > latest) {
			destor_log(DESTOR_DEBUG, "Latest segment: %lld -> %lld", latest,
					id);
			latest = id;
		} else if (id != TEMPORARY_ID) {
			destor_log(DESTOR_DEBUG, "Latest segment: %lld -> %lld failed",
					latest, id);
		} else {
			unsigned char code[41];
			hash2code(key, code);
			code[40] = 0;
			DEBUG("Feature %s missed", code);
		}
	}

	/* If latest has already been cached, we need not to read it. */
	if (latest != TEMPORARY_ID
			&& !lru_cache_hits(segment_recipe_cache, &latest,
					segment_recipe_check_id)) {
		jcr.index_lookup_io++;
		GQueue* segments = prefetch_segments(latest,
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

	}

}

/*
 * Larger one comes before smaller one.
 * Descending order.
 */
static gint g_segment_cmp_feature_num(struct segmentRecipe* a,
		struct segmentRecipe* b, gpointer user_data) {
	gint ret = g_hash_table_size(b->features) - g_hash_table_size(a->features);
	if(ret == 0)
		return b->id - a->id;
	else
		return ret;
}

/*
 * Remove the features that are common with top from target.
 */
static void features_trim(struct segmentRecipe *target,
		struct segmentRecipe *top) {
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, top->features);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		g_hash_table_remove(target->features, key);
	}
}

/*
 * Select the top segments that are most similar with features.
 * (top-k * prefetching_num) cannot be larger than the segment cache size.
 */
static void top_segment_select(GHashTable* features) {
	/*
	 * Mapping segment IDs to similar segments that hold at least one of features.
	 */
	GHashTable *table = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL,
			free_segment_recipe);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, features);
	/* Iterate the features of the segment. */
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		/* Each feature is mapped to several segment IDs. */
		segmentid *ids = feature_index_lookup((fingerprint*) key);
		if (ids) {
			int i;
			for (i = 0; i < destor.index_feature_segment_num; i++) {
				if (ids[i] == TEMPORARY_ID)
					break;
				struct segmentRecipe* sr = g_hash_table_lookup(table, &ids[i]);
				if (!sr) {
					sr = new_segment_recipe();
					sr->id = ids[i];
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

		/* Sorting similar segments in order of their number of hit features. */
		GSequence *seq = g_sequence_new(NULL);
		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, table);
		while (g_hash_table_iter_next(&iter, &key, &value)) {
			/* Insert similar segments into GSequence. */
			g_sequence_insert_sorted(seq, (struct segmentRecipe*) value,
					g_segment_cmp_feature_num,
					NULL);
		}

		/* The number of selected similar segments */
		int num =
				g_sequence_get_length(seq)
						> destor.index_segment_selection_method[1] ?
						destor.index_segment_selection_method[1] :
						g_sequence_get_length(seq), i;

		DEBUG("select Top-%d in %d segments\n", num, g_sequence_get_length(seq));

		/* Prefetched top similar segments are pushed into the queue. */
		GQueue *segments = g_queue_new();
		for (i = 0; i < num; i++) {
			/* Get the top segment */
			struct segmentRecipe *top = g_sequence_get(
					g_sequence_get_begin_iter(seq));

			/*
			 * If it doesn't exist in the cache,
			 * we need to insert it into the cache.
			 * A segment of higher rank is inserted later.
			 *  */
			if (!lru_cache_hits(segment_recipe_cache, &top->id,
					segment_recipe_check_id)) {

				jcr.index_lookup_io++;
				/* We prefetch the segments adjacent to the top. */
				GQueue* tmp = prefetch_segments(top->id,
						destor.index_segment_prefech);
				struct segmentRecipe* sr = g_queue_pop_head(tmp);
				while (sr) {
					if (g_queue_find_custom(segments, &sr->id,
							segment_recipe_check_id))
						VERBOSE(
								"Dedup phase: prefetching a segment already read! Top selection");

					g_queue_push_tail(segments, sr);
					sr = g_queue_pop_head(tmp);
				}
				g_queue_free(tmp);
			}

			g_sequence_remove(g_sequence_get_begin_iter(seq));
			g_sequence_foreach(seq, features_trim, top);
			g_sequence_sort(seq, g_segment_cmp_feature_num, NULL);
		}
		g_sequence_free(seq);

		struct segmentRecipe* sr;
		while ((sr = g_queue_pop_tail(segments))) {
			assert(sr);
			lru_cache_insert(segment_recipe_cache, sr, NULL, NULL);
		}

		g_queue_free(segments);
	}

	g_hash_table_destroy(table);
}

static void all_segment_select(GHashTable* features) {
	struct segmentRecipe* selected = NULL;

	GHashTable *score_table = g_hash_table_new_full(g_int64_hash, g_int64_equal,
	NULL, free);
	segmentid champion[2] = { TEMPORARY_ID, 0 };
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, features);
	/* Iterate the features of the segment. */
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		/* Each feature is mapped to several segment IDs. */
		segmentid *ids = feature_index_lookup((fingerprint*) key);
		if (ids) {
			int i;
			for (i = 0; i < destor.index_feature_segment_num; i++) {
				if (ids[i] == TEMPORARY_ID)
					break;
				segmentid* segscore = g_hash_table_lookup(score_table, &ids[i]);
				if (!segscore) {
					segscore = malloc(sizeof(segmentid) * 2);
					segscore[0] = ids[i];
					segscore[1] = 0;
					g_hash_table_insert(score_table, &segscore[0], segscore);
				}
				segscore[1]++;
				if (segscore[0] != champion[0] && segscore[1] > champion[1]) {
					/* new champion */
					champion[0] = segscore[0];
					champion[1] = segscore[0];
				}
			}
		} else {
			VERBOSE("Dedup phase: Missed feature");
		}
	}
	g_hash_table_destroy(score_table);

	if (champion[0] != TEMPORARY_ID) {
		/* We have a champion */
		selected = lru_cache_hits(segment_recipe_cache, &champion[0],
				segment_recipe_check_id);
		if (!selected) {
			/* It is not in the cache */
			GList *elem = g_queue_find_custom(segment_buffer, &champion[0],
					segment_recipe_check_id);
			if (!elem) {
				jcr.index_lookup_io++;
				TIMER_DECLARE(1);
				TIMER_BEGIN(1);
				selected = retrieve_segment_all_in_one(champion[0]);
				TIMER_END(1, read_segment_time);
				lru_cache_insert(segment_recipe_cache, selected, NULL, NULL);
			} else {
				/* It is in the buffer but not in the cache*/
				selected = elem->data;
				lru_cache_insert(segment_recipe_cache,
						ref_segment_recipe(selected),
						NULL, NULL);
			}
		}
		g_queue_push_tail(segment_buffer, ref_segment_recipe(selected));
	} else {
		NOTICE("Dedup phase: No similar segment is selected. %d features.",
				g_hash_table_size(features));
		selected = new_segment_recipe();
		g_queue_push_tail(segment_buffer, selected);
	}
}

void near_exact_similarity_index_lookup(struct segment* s) {
	/* Load similar segments into segment cache. */
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

		if (CHECK_CHUNK(c,
				CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
			continue;

		GQueue *tq = g_hash_table_lookup(index_buffer.table, &c->fp);
		if (tq) {
			struct indexElem *be = g_queue_peek_head(tq);
			c->id = be->id;
			SET_CHUNK(c, CHUNK_DUPLICATE);
		} else {
			tq = g_queue_new();
		}

		if (!CHECK_CHUNK(c, CHUNK_DUPLICATE) && segment_recipe_cache) {
			struct segmentRecipe* si = lru_cache_lookup(segment_recipe_cache,
					&c->fp);
			if (si) {
				/* Find it */
				struct indexElem *ie = g_hash_table_lookup(si->index, c->fp);
				assert(ie);
				SET_CHUNK(c, CHUNK_DUPLICATE);
				c->id = ie->id;
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

containerid near_exact_similarity_index_update(fingerprint *fp,
		containerid from, containerid to) {
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
		/* 'to' is meaningless. */
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
		} else {
			/* a normal redundant chunk */
			assert(to!=TEMPORARY_ID);
		}
	}

	struct indexElem * be = (struct indexElem*) malloc(
			sizeof(struct indexElem));
	be->id = final_id == TEMPORARY_ID ? to : final_id;
	memcpy(&be->fp, fp, sizeof(fingerprint));
	g_hash_table_replace(srbuf->index, &be->fp, be);

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

		if (bs->features) {
			GHashTableIter iter;
			gpointer key, value;
			g_hash_table_iter_init(&iter, bs->features);
			while (g_hash_table_iter_next(&iter, &key, &value)) {
				if (!g_hash_table_contains(srbuf->features, key)) {
					fingerprint* f = (fingerprint*) malloc(sizeof(fingerprint));
					memcpy(f, key, sizeof(fingerprint));
					g_hash_table_insert(srbuf->features, f, f);
				}
			}
		}

		free_segment(bs, free);

		jcr.index_update_io++;
		if (destor.index_segment_selection_method[0] == INDEX_SEGMENT_SELECT_ALL) {
			struct segmentRecipe* base = g_queue_pop_head(segment_buffer);
			/* over-write old addresses. */
			base = segment_recipe_merge(base, srbuf);
			unref_segment_recipe(srbuf);
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
		gpointer key, value;
		g_hash_table_iter_init(&iter, srbuf->features);
		while (g_hash_table_iter_next(&iter, &key, &value))
			feature_index_update((fingerprint*) key, srbuf->id);

		unref_segment_recipe(srbuf);
		srbuf = NULL;
		n = 0;
	}

	return final_id;
}
