#include "index.h"

/* g_mutex_init() is unnecessary if in static storage. */
static GMutex mutex;
static GCond not_empty_cond; // buffer is not empty
static GCond not_full_cond; // buffer is not full

GHashTable* (*featuring)(fingerprint *fp, int success);

/*
 * Used by Extreme Binning and Silo.
 */
static GHashTable* index_feature_min(fingerprint *fp, int success) {
	static GHashTable* features = NULL;
	static int count = 0;
	static fingerprint candidate;

	if (fp == NULL && success != 1)
		return NULL;

	/* Init */
	if (count == 0)
		memset(&candidate, 0xff, sizeof(fingerprint));
	if (features == NULL)
		features = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
				free, NULL);

	/* New fingerprint */
	if (fp) {
		if (memcmp(fp, &candidate, sizeof(fingerprint)) < 0) {
			memcpy(&candidate, fp, sizeof(fingerprint));
		}
		count++;
		if (count == destor.index_feature_method[1]) {
			/*
			 * Select a feature per destor.index_feature_method[1]
			 * */
			if (!g_hash_table_contains(features, &candidate)) {
				fingerprint *new_feature = (fingerprint*) malloc(
						sizeof(fingerprint));
				memcpy(new_feature, &candidate, sizeof(fingerprint));
				g_hash_table_insert(features, new_feature, new_feature);
			}
			memset(&candidate, 0xff, sizeof(fingerprint));
			count = 0;

		}
	}

	if (success) {
		/* a segment/container boundary. */
		if (g_hash_table_size(features) == 0
				|| count * 2 > destor.index_feature_method[1]) {
			/*
			 * Already considering the case of destor.index_feature_method[1] being zero.
			 */
			if (!g_hash_table_contains(features, &candidate)) {
				fingerprint *new_feature = (fingerprint*) malloc(
						sizeof(fingerprint));
				memcpy(new_feature, &candidate, sizeof(fingerprint));
				g_hash_table_insert(features, new_feature, new_feature);
			}
		}

		GHashTable *f = features;
		features = NULL;
		memset(&candidate, 0xff, sizeof(fingerprint));
		count = 0;
		return f;
	}

	return NULL;
}

/*
 * Used by Sparse Indexing.
 */
static GHashTable* index_feature_sample(fingerprint *fp, int success) {
	static GHashTable* features = NULL;

	if (fp == NULL && success != 1)
		return NULL;

	if (destor.index_feature_method[0] == INDEX_FEATURE_NO)
		return NULL;

	if (features == NULL)
		features = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
				free, NULL);

	assert(destor.index_feature_method[1] != 0);
	if (fp) {
		if ((*((int*) fp)) % destor.index_feature_method[1] == 0) {
			if (!g_hash_table_contains(features, fp)) {
				fingerprint *new_feature = (fingerprint*) malloc(
						sizeof(fingerprint));
				memcpy(new_feature, fp, sizeof(fingerprint));
				g_hash_table_insert(features, new_feature, new_feature);
			}
		}
	}

	if (success) {
		if (g_hash_table_size(features) == 0) {
			/* No feature? */
			fingerprint *new_feature = (fingerprint*) malloc(
					sizeof(fingerprint));
			if (fp)
				memcpy(new_feature, fp, sizeof(fingerprint));
			else
				memset(new_feature, 0x00, sizeof(fingerprint));
			g_hash_table_insert(features, new_feature, new_feature);
		}
		GHashTable* f = features;
		features = NULL;
		return f;
	}

	return NULL;
}

static GHashTable* index_feature_uniform(fingerprint *fp, int success) {
	static GHashTable* features = NULL;
	static int count;

	if (fp == NULL && success != 1)
		return NULL;

	if (destor.index_feature_method[0] == INDEX_FEATURE_NO)
		return NULL;

	if (features == NULL)
		features = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
				free, NULL);

	assert(destor.index_feature_method[1] != 0);
	if (fp) {
		if (count % destor.index_feature_method[1] == 0) {
			if (!g_hash_table_contains(features, fp)) {
				fingerprint *new_feature = (fingerprint*) malloc(
						sizeof(fingerprint));
				memcpy(new_feature, fp, sizeof(fingerprint));
				g_hash_table_insert(features, new_feature, new_feature);
			}
			count = 0;
		}
		count++;
	}

	if (success) {
		if (g_queue_get_length(features) == 0) {
			/* No feature? Empty segment.*/
			assert(fp == NULL);
			fingerprint *new_feature = (fingerprint*) malloc(
					sizeof(fingerprint));
			memset(new_feature, 0x00, sizeof(fingerprint));
			g_hash_table_insert(features, new_feature, new_feature);
		}
		GHashTable* f = features;
		features = NULL;
		return f;
	}

	return NULL;
}

void init_index() {
	index_buffer.segment_queue = g_queue_new();
	index_buffer.table = g_hash_table_new_full(g_int64_hash, g_fingerprint_cmp,
	NULL, g_queue_free);

	switch (destor.index_feature_method[0]) {
	case INDEX_FEATURE_SAMPLE:
	case INDEX_FEATURE_NO:
		featuring = index_feature_sample;
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

	init_near_exact_locality_index();
}

void close_index() {
	assert(g_queue_get_length(index_buffer.segment_queue) == 0);
	assert(g_hash_table_size(index_buffer.table) == 0);
	close_near_exact_locality_index();
}

/*
 * Call index_lookup() to obtain container IDs of chunks in a segment.
 * Their fingerprints and container IDs are inserted into index_buffer.
 */
void index_lookup(struct segment* s) {
	g_mutex_lock(&mutex);

	int len = g_queue_get_length(index_buffer.segment_queue);
	/* Ensure the next phase not be blocked. */
	if (len > 2 * destor.rewrite_algorithm[1])
		g_cond_wait(&not_full_cond, &mutex);

	near_exact_locality_index_lookup(s);

	g_cond_broadcast(&not_empty_cond);

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
 * Return CONTAINER_TMP_ID if to is the final id, otherwise return e->id.
 */
int index_update(fingerprint fp, containerid from, containerid to) {

	g_mutex_lock(&mutex);

	if (g_queue_get_length(index_buffer.segment_queue) == 0) {
		g_cond_wait(&not_empty_cond, &mutex);
	}

	int len1 = g_queue_get_length(index_buffer.segment_queue);

	containerid final_id = near_exact_locality_index_update(fp, from, to);

	int len2 = g_queue_get_length(index_buffer.segment_queue);

	if (len1 > len2)
		g_cond_broadcast(&not_full_cond);

	g_mutex_unlock(&mutex);

	return final_id;
}

struct segmentRecipe* new_segment_recipe() {
	struct segmentRecipe* sr = (struct segmentRecipe*) malloc(
			sizeof(struct segmentRecipe));
	sr->id = TEMPORARY_ID;
	sr->table = g_hash_table_new_full(g_int64_hash, g_fingerprint_cmp, NULL,
			free);
	sr->features = g_hash_table_new_full(g_int64_hash, g_fingerprint_cmp, free,
	NULL);
	return sr;
}

void free_segment_recipe(struct segmentRecipe* sr) {
	g_hash_table_destroy(sr->table);
	g_hash_table_destroy(sr->features);
	free(sr);
}

int lookup_fingerprint_in_segment_recipe(struct segmentRecipe* sr,
		fingerprint *fp) {
	return g_hash_table_lookup(sr->table, fp) == NULL ? 0 : 1;
}

int segment_recipe_check_id(struct segmentRecipe* sr, segmentid *id) {
	return sr->id == *id;
}

struct segmentRecipe* segment_recipe_dup(struct segmentRecipe* sr) {
	struct segmentRecipe* dup = new_segment_recipe();

	dup->id = sr->id;
	GHashTableIter iter;
	fingerprint *key, *value;
	g_hash_table_iter_init(&iter, sr->features);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		fingerprint *feature = (fingerprint*) malloc(sizeof(fingerprint));
		memcpy(feature, key, sizeof(fingerprint));
		g_hash_table_insert(dup->features, feature, feature);
	}

	struct indexElem* ie;
	g_hash_table_iter_init(&iter, sr->table);
	while (g_hash_table_iter_next(&iter, &key, &ie)) {
		struct indexElem* elem = (struct indexElem*) malloc(
				sizeof(struct indexElem));
		memcpy(elem, ie, sizeof(struct indexElem));
		g_hash_table_insert(dup->table, &elem->fp, elem);
	}
	return dup;
}
