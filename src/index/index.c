#include "index.h"
#include "../jcr.h"
#include "exact_locality_index.h"
#include "exact_similarity_index.h"
#include "near_exact_locality_index.h"
#include "near_exact_similarity_index.h"

/* g_mutex_init() is unnecessary if in static storage. */
static GMutex mutex;
static GCond not_full_cond; // buffer is not full
static int wait_flag;

/*
 * Calculate features for a chunk sequence.
 */
GHashTable* (*featuring)(GQueue *chunks, int32_t chunk_num);

/*
 * Used by Extreme Binning and Silo.
 */
static GHashTable* index_feature_min(GQueue *chunks, int32_t chunk_num) {

	chunk_num = (chunk_num == 0) ? g_queue_get_length(chunks) : chunk_num;
	int feature_num = 1;
	if (destor.index_feature_method[1] != 0
			&& chunk_num > destor.index_feature_method[1]) {
		/* Calculate the number of features we need */
		int remain = chunk_num % destor.index_feature_method[1];
		feature_num = chunk_num / destor.index_feature_method[1];
		feature_num =
				(remain * 2 > destor.index_feature_method[1]) ?
						feature_num + 1 : feature_num;
	}

	GSequence *candidates = g_sequence_new(NULL);
	int queue_len = g_queue_get_length(chunks), i;
	for (i = 0; i < queue_len; i++) {
		/* iterate the queue */
		struct chunk* c = g_queue_peek_nth(chunks, i);

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
			continue;

		if (g_sequence_get_length(candidates) < feature_num
				|| memcmp(&c->fp,
						g_sequence_get(
								g_sequence_iter_prev(
										g_sequence_get_end_iter(candidates))),
						sizeof(fingerprint)) < 0) {
			/* insufficient candidates or new candidate */
			fingerprint *new_candidate = (fingerprint*) malloc(
					sizeof(fingerprint));
			memcpy(new_candidate, &c->fp, sizeof(fingerprint));
			g_sequence_insert_sorted(candidates, new_candidate,
					g_fingerprint_cmp, NULL);
			if (g_sequence_get_length(candidates) > feature_num) {
				free(
						g_sequence_get(
								g_sequence_iter_prev(
										g_sequence_get_end_iter(candidates))));
				g_sequence_remove(
						g_sequence_iter_prev(
								g_sequence_get_end_iter(candidates)));
			}
		}
	}

	GHashTable * features = g_hash_table_new_full(g_int64_hash,
			g_fingerprint_equal, free, NULL);

	fingerprint *feature = NULL;
	while (g_sequence_get_length(candidates) > 0) {
		fingerprint *feature = g_sequence_get(
				g_sequence_get_begin_iter(candidates));
		g_hash_table_replace(features, feature, NULL);
		g_sequence_remove(g_sequence_get_begin_iter(candidates));
	}
	g_sequence_foreach(candidates, free, NULL);
	g_sequence_remove_range(g_sequence_get_begin_iter(candidates),
			g_sequence_get_end_iter(candidates));

	if (g_hash_table_size(features) == 0) {
		WARNING(
				"Dedup phase: An empty segment and thus no min-feature is selected!");
		fingerprint* fp = malloc(sizeof(fingerprint));
		memset(fp, 0xff, sizeof(fingerprint));
		g_hash_table_insert(features, fp, NULL);
	}

	return features;
}

/*
 * Used by Sparse Indexing.
 */
static GHashTable* index_feature_random(GQueue *chunks, int32_t chunk_num) {
	assert(destor.index_feature_method[1] != 0);
	GHashTable * features = g_hash_table_new_full(g_int64_hash,
			g_fingerprint_equal, free, NULL);

	int queue_len = g_queue_get_length(chunks), i;
	for (i = 0; i < queue_len; i++) {
		/* iterate the queue */
		struct chunk* c = g_queue_peek_nth(chunks, i);

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
			continue;

		if ((*((int*) (&c->fp))) % destor.index_feature_method[1] == 0) {
			if (!g_hash_table_contains(features, &c->fp)) {
				fingerprint *new_feature = (fingerprint*) malloc(
						sizeof(fingerprint));
				memcpy(new_feature, &c->fp, sizeof(fingerprint));
				g_hash_table_insert(features, new_feature,
				NULL);
			}
		}
	}

	if (g_hash_table_size(features) == 0) {
		/* No feature? */
		WARNING("Dedup phase: no features are sampled");
		fingerprint *new_feature = (fingerprint*) malloc(sizeof(fingerprint));
		memset(new_feature, 0x00, sizeof(fingerprint));
		g_hash_table_insert(features, new_feature, NULL);
	}
	return features;

}

static GHashTable* index_feature_uniform(GQueue *chunks, int32_t chunk_num) {
	assert(destor.index_feature_method[1] != 0);
	GHashTable * features = g_hash_table_new_full(g_int64_hash,
			g_fingerprint_equal, free, NULL);
	int count = 0;
	int queue_len = g_queue_get_length(chunks), i;
	for (i = 0; i < queue_len; i++) {
		struct chunk *c = g_queue_peek_nth(chunks, i);
		/* Examine whether fp is a feature */
		if (count % destor.index_feature_method[1] == 0) {
			if (!g_hash_table_contains(features, &c->fp)) {
				fingerprint *new_feature = (fingerprint*) malloc(
						sizeof(fingerprint));
				memcpy(new_feature, &c->fp, sizeof(fingerprint));
				g_hash_table_insert(features, new_feature,
				NULL);
			}
		}
		count++;
	}

	if (g_hash_table_size(features) == 0) {
		/* No feature? Empty segment.*/
		assert(chunk_num == 0);
		WARNING(
				"Dedup phase: An empty segment and thus no uniform-feature is selected!");
		fingerprint *new_feature = (fingerprint*) malloc(sizeof(fingerprint));
		memset(new_feature, 0x00, sizeof(fingerprint));
		g_hash_table_insert(features, new_feature,
		NULL);
	}
	return features;
}

void init_index() {
	index_buffer.segment_queue = g_queue_new();
	/* Do NOT assign a free function for value. */
	index_buffer.table = g_hash_table_new_full(g_int64_hash,
			g_fingerprint_equal,
			NULL, NULL);
	index_buffer.num = 0;

	index_buffer.feature_buffer = g_queue_new();
	index_buffer.cid = TEMPORARY_ID;

	if (destor.index_category[0] == INDEX_CATEGORY_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY) {
		init_exact_locality_index();
	} else if (destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY)
		init_near_exact_locality_index();
	else if (destor.index_category[0] == INDEX_CATEGORY_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY)
		init_exact_similarity_index();
	else if (destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY)
		init_near_exact_similarity_index();
	else {
		fprintf(stderr, "Invalid fingerprint category");
		exit(1);
	}

	switch (destor.index_feature_method[0]) {
	case INDEX_FEATURE_RANDOM:
	case INDEX_FEATURE_NO:
		featuring = index_feature_random;
		break;
	case INDEX_FEATURE_MIN:
		featuring = index_feature_min;
		break;
	case INDEX_FEATURE_UNIFORM:
		featuring = index_feature_uniform;
		break;
	default:
		fprintf(stderr, "Invalid feature method!\n");
		exit(1);
	}
}

void close_index() {
	assert(g_queue_get_length(index_buffer.segment_queue) == 0);
	assert(g_hash_table_size(index_buffer.table) == 0);

	if (destor.index_category[0] == INDEX_CATEGORY_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY)
		close_exact_locality_index();
	else if (destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY)
		close_near_exact_locality_index();
	else if (destor.index_category[0] == INDEX_CATEGORY_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY)
		close_exact_similarity_index();
	else if (destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY)
		close_near_exact_similarity_index();
	else {
		fprintf(stderr, "Invalid fingerprint category");
		exit(1);
	}
}

/*
 * Call index_lookup() to obtain container IDs of chunks in a segment.
 * Their fingerprints and container IDs are inserted into index_buffer.
 */
void index_lookup(struct segment* s) {
	g_mutex_lock(&mutex);

	/* Ensure the next phase not be blocked. */
	if (index_buffer.num > 2 * destor.rewrite_algorithm[1]) {
		DEBUG("The index buffer is full (%d chunks in buffer)",
				index_buffer.num);
		wait_flag = 1;
		g_cond_wait(&not_full_cond, &mutex);
	}

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	TIMER_DECLARE(2);
	TIMER_BEGIN(2);

	if (destor.index_category[0] == INDEX_CATEGORY_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY)
		exact_locality_index_lookup(s);
	else if (destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY)
		near_exact_locality_index_lookup(s);
	else if (destor.index_category[0] == INDEX_CATEGORY_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY)
		exact_similarity_index_lookup(s);
	else if (destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY) {
		if (destor.index_segment_selection_method[0]
				== INDEX_SEGMENT_SELECT_GREEDY)
			near_exact_similarity_index_lookup_greedy(s);
		else
			near_exact_similarity_index_lookup(s);
	} else {
		fprintf(stderr, "Invalid fingerprint category");
		exit(1);
	}

	TIMER_END(2, jcr.index_lookup_time);
	TIMER_END(1, jcr.dedup_time);

	g_mutex_unlock(&mutex);
}

/*
 * Update old id (from) to new id (to), with a buffered id (e->id).
 * Only if from >= e->id && to > from, we update the index.
 * We are sure: e->id >= from >= to
 *
 * 1. from == e->id, update it and return CONTAINER_TMP_ID.
 * 		1.1 if from == CONTAINER_TMP_ID, it is a unique chunk.
 * 			assert(to != CONTAINER_TMP_ID)
 * 		1.2 if from == to, it is a duplicate chunk.
 * 		1.3 if from != CONTAINER_TMP_ID && from != to, it is a rewritten chunk.
 *
 * 2. from < e->id, it has been written recently. Refusing the update, and return e->id.
 * 		2.1 if from == CONTAINER_TMP_ID, it is a duplicate chunk but reference a unique chunk.
 * 			assert(to == CONTAINER_TMP_ID)
 * 		2.2 if from != CONTAINER_TMP_ID, it has been rewritten recently.
 *
 * Return TEMPORARY_ID if to is the final id, otherwise return e->id.
 */
int index_update(fingerprint *fp, containerid from, containerid to) {

	g_mutex_lock(&mutex);

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	TIMER_DECLARE(2);
	TIMER_BEGIN(2);

	containerid final_id;

	if (destor.index_category[0] == INDEX_CATEGORY_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY)
		final_id = exact_locality_index_update(fp, from, to);
	else if (destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY)
		final_id = near_exact_locality_index_update(fp, from, to);
	else if (destor.index_category[0] == INDEX_CATEGORY_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY)
		final_id = exact_similarity_index_update(fp, from, to);
	else if (destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY)
		final_id = near_exact_similarity_index_update(fp, from, to);
	else {
		fprintf(stderr, "Invalid fingerprint category");
		exit(1);
	}

	if (wait_flag == 1 && index_buffer.num <= 2 * destor.rewrite_algorithm[1]) {
		DEBUG("The index buffer is ready for more chunks (%d chunks in buffer)",
				index_buffer.num);
		wait_flag = 0;
		g_cond_broadcast(&not_full_cond);
	}

	TIMER_END(2, jcr.index_update_time);
	TIMER_END(1, jcr.filter_time);

	g_mutex_unlock(&mutex);

	return final_id;
}

struct segmentRecipe* new_segment_recipe() {
	struct segmentRecipe* sr = (struct segmentRecipe*) malloc(
			sizeof(struct segmentRecipe));
	sr->id = TEMPORARY_ID;
	sr->index = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
	NULL, free);
	sr->features = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
			free, NULL);
	sr->reference_count = 1;
	pthread_mutex_init(&sr->mutex, NULL);
	return sr;
}

/* For simple, ref and unref cannot be called concurrently */
struct segmentRecipe* ref_segment_recipe(struct segmentRecipe* sr) {
	pthread_mutex_lock(&sr->mutex);

	sr->reference_count++;

	pthread_mutex_unlock(&sr->mutex);
	return sr;
}

void unref_segment_recipe(struct segmentRecipe* sr) {
	pthread_mutex_lock(&sr->mutex);
	sr->reference_count++;
	if (sr->reference_count == 0) {
		free_segment_recipe(sr);
		return;
	}
	pthread_mutex_unlock(&sr->mutex);
}

void free_segment_recipe(struct segmentRecipe* sr) {
	g_hash_table_destroy(sr->index);
	g_hash_table_destroy(sr->features);
	pthread_mutex_destroy(&sr->mutex);
	free(sr);
}

int lookup_fingerprint_in_segment_recipe(struct segmentRecipe* sr,
		fingerprint *fp) {
	return g_hash_table_lookup(sr->index, fp) == NULL ? 0 : 1;
}

int segment_recipe_check_id(struct segmentRecipe* sr, segmentid *id) {
	return sr->id == *id;
}

/*
 * Duplicate a segmentRecipe.
 */
struct segmentRecipe* segment_recipe_dup(struct segmentRecipe* sr) {
	struct segmentRecipe* dup = new_segment_recipe();

	dup->id = sr->id;
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, sr->features);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		fingerprint *feature = (fingerprint*) malloc(sizeof(fingerprint));
		memcpy(feature, key, sizeof(fingerprint));
		g_hash_table_insert(dup->features, feature, feature);
	}

	g_hash_table_iter_init(&iter, sr->index);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct indexElem* elem = (struct indexElem*) malloc(
				sizeof(struct indexElem));
		memcpy(elem, value, sizeof(struct indexElem));
		g_hash_table_insert(dup->index, &elem->fp, elem);
	}
	return dup;
}

/*
 * Merge a segmentRecipe into a base segmentRecipe.
 * Only AIO will call the function.
 */
struct segmentRecipe* segment_recipe_merge(struct segmentRecipe* base,
		struct segmentRecipe* delta) {
	/*
	 * Select the larger id,
	 * which indicating a later or larger segment.
	 */
	base->id = base->id > delta->id ? base->id : delta->id;

	/* Iterate features in delta */
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, delta->features);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		if (!g_hash_table_contains(base->features, key)) {
			fingerprint *feature = (fingerprint*) malloc(sizeof(fingerprint));
			memcpy(feature, key, sizeof(fingerprint));
			g_hash_table_insert(base->features, feature, feature);
		}
	}

	/* Iterate fingerprints in delta */
	g_hash_table_iter_init(&iter, delta->index);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct indexElem* base_elem = g_hash_table_lookup(base->index, key);
		if (!base_elem) {
			base_elem = (struct indexElem*) malloc(sizeof(struct indexElem));
			memcpy(base_elem, value, sizeof(struct indexElem));
			g_hash_table_insert(base->index, &base_elem->fp, base_elem);
		} else {
			/* Select the newer id. More discussions are required. */
			struct indexElem* ie = (struct indexElem*) value;
			base_elem->id = base_elem->id >= ie->id ? base_elem->id : ie->id;
		}
	}

	return base;
}
