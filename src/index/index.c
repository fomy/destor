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
 * Inputs: a fingerprint,
 * 		   a success flag that indicates whether a segment is terminated.
 * If success is true,
 * a segment is terminated and thus we return the features.
 *
 * featuring(fp, 0): a normal fingerprint, not a segment boundary,
 * 	examine whether fp is a feature and return nothing;
 * featuring(fp, 1): a normal fingerprint, a segment boundary,
 * 	examine whether fp is a feature and return features;
 * featuring(NULL, 0): a Flag, not a segment boundary, no operation and no return;
 * featuring(NULL, 1): a flag, a segment boundary
 * 	(maybe no selected features, select a predefined value as a feature).
 */
GHashTable* (*featuring)(fingerprint *fp, int success);

/*
 * Used by Extreme Binning and Silo.
 */
static GHashTable* index_feature_min(fingerprint *fp, int success) {
	static int count = 0;
	static fingerprint candidate;

	if (!fp && !success)
		return NULL;

	/* Init */
	if (count == 0)
		memset(&candidate, 0xff, sizeof(fingerprint));
	if (index_buffer.buffered_features == NULL)
		index_buffer.buffered_features = g_hash_table_new_full(g_int64_hash,
				g_fingerprint_equal, free, NULL);

	/* Examine whether fp is a feature */
	if (fp) {
		if (memcmp(fp, &candidate, sizeof(fingerprint)) < 0)
			memcpy(&candidate, fp, sizeof(fingerprint));

		count++;
		if (count == destor.index_feature_method[1]) {
			/*
			 * Select a feature per destor.index_feature_method[1]
			 * */
			if (!g_hash_table_contains(index_buffer.buffered_features,
					&candidate)) {
				fingerprint *new_feature = (fingerprint*) malloc(
						sizeof(fingerprint));
				memcpy(new_feature, &candidate, sizeof(fingerprint));
				g_hash_table_insert(index_buffer.buffered_features, new_feature,
						new_feature);
			}
			count = 0;
		}
	}

	if (success) {
		/* a segment/container boundary. */
		if (g_hash_table_size(index_buffer.buffered_features) == 0
				|| count * 2 > destor.index_feature_method[1]) {
			/*
			 * Already considering the case of destor.index_feature_method[1] being zero.
			 */
			if (!g_hash_table_contains(index_buffer.buffered_features,
					&candidate)) {
				fingerprint *new_feature = (fingerprint*) malloc(
						sizeof(fingerprint));
				memcpy(new_feature, &candidate, sizeof(fingerprint));
				g_hash_table_insert(index_buffer.buffered_features, new_feature,
						new_feature);
			}
		}

		count = 0;
		GHashTable* ret = index_buffer.buffered_features;
		index_buffer.buffered_features = NULL;
		return ret;
	}

	return NULL;
}

/*
 * Used by Sparse Indexing.
 */
static GHashTable* index_feature_random(fingerprint *fp, int success) {

	assert(destor.index_feature_method[1] != 0);

	if (!fp && !success)
		return NULL;

	if (destor.index_feature_method[0] == INDEX_FEATURE_NO)
		return NULL;

	if (index_buffer.buffered_features == NULL)
		index_buffer.buffered_features = g_hash_table_new_full(g_int64_hash,
				g_fingerprint_equal, free, NULL);

	/* Examine whether fp is a feature */
	assert(destor.index_feature_method[1] != 0);
	if (fp) {
		if ((*((int*) fp)) % destor.index_feature_method[1] == 0) {
			if (!g_hash_table_contains(index_buffer.buffered_features, fp)) {
				fingerprint *new_feature = (fingerprint*) malloc(
						sizeof(fingerprint));
				memcpy(new_feature, fp, sizeof(fingerprint));
				g_hash_table_insert(index_buffer.buffered_features, new_feature,
						new_feature);
			}
		}
	}

	/* A segment boundary */
	if (success) {
		if (g_hash_table_size(index_buffer.buffered_features) == 0) {
			/* No feature? */
			fingerprint *new_feature = (fingerprint*) malloc(
					sizeof(fingerprint));
			if (fp)
				memcpy(new_feature, fp, sizeof(fingerprint));
			else
				memset(new_feature, 0x00, sizeof(fingerprint));
			g_hash_table_insert(index_buffer.buffered_features, new_feature,
					new_feature);
		}
		GHashTable* ret = index_buffer.buffered_features;
		index_buffer.buffered_features = NULL;
		return ret;
	}

	return NULL;
}

static GHashTable* index_feature_uniform(fingerprint *fp, int success) {
	assert(destor.index_feature_method[1] != 0);

	static int count;

	if (!fp && !success)
		return NULL;

	if (destor.index_feature_method[0] == INDEX_FEATURE_NO)
		return NULL;

	if (index_buffer.buffered_features == NULL)
		index_buffer.buffered_features = g_hash_table_new_full(g_int64_hash,
				g_fingerprint_equal, free, NULL);

	/* Examine whether fp is a feature */
	assert(destor.index_feature_method[1] != 0);
	if (fp) {
		if (count % destor.index_feature_method[1] == 0) {
			if (!g_hash_table_contains(index_buffer.buffered_features, fp)) {
				fingerprint *new_feature = (fingerprint*) malloc(
						sizeof(fingerprint));
				memcpy(new_feature, fp, sizeof(fingerprint));
				g_hash_table_insert(index_buffer.buffered_features, new_feature,
						new_feature);
			}
			count = 0;
		}
		count++;
	}

	/* A segment boundary */
	if (success) {
		if (g_hash_table_size(index_buffer.buffered_features) == 0) {
			/* No feature? Empty segment.*/
			assert(fp == NULL);
			fingerprint *new_feature = (fingerprint*) malloc(
					sizeof(fingerprint));
			memset(new_feature, 0x00, sizeof(fingerprint));
			g_hash_table_insert(index_buffer.buffered_features, new_feature,
					new_feature);
		}
		GHashTable* ret = index_buffer.buffered_features;
		index_buffer.buffered_features = NULL;
		return ret;
	}

	return NULL;
}

void init_index() {
	index_buffer.segment_queue = g_queue_new();
	/* Do NOT assign a free function for value. */
	index_buffer.table = g_hash_table_new_full(g_int64_hash,
			g_fingerprint_equal,
			NULL, NULL);
	index_buffer.num = 0;

	index_buffer.buffered_features = NULL;
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
			&& destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY)
		near_exact_similarity_index_lookup(s);
	else {
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
	sr->index = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL,
			free);
	sr->features = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
			free, NULL);
	return sr;
}

void free_segment_recipe(struct segmentRecipe* sr) {
	g_hash_table_destroy(sr->index);
	g_hash_table_destroy(sr->features);
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
