/*
 * feature_index.c
 *
 *  Created on: Nov 21, 2013
 *      Author: fumin
 */

#include "feature_index.h"

struct featureIndexElem {
	fingerprint feature;
	GQueue* ids;
};

static GHashTable *feature_index;
static int max_id_num_per_feature;

static struct featureIndexElem* new_feature_index_elem() {
	struct featureIndexElem* e = (struct featureIndexElem*) malloc(
			sizeof(struct featureIndexElem));

	e->ids = g_queue_new();

	return e;
}

static void free_feature_index_elem(struct featureIndexElem* e) {
	g_queue_free_full(e->ids, free);
	free(e);
}

static void feature_index_elem_add_id(struct featureIndexElem* ie, int64_t id) {
	int64_t* new_id = (int64_t*) malloc(sizeof(int64_t));
	*new_id = id;
	g_queue_push_tail(ie->ids, new_id);
	if (g_queue_get_length(ie->ids) > max_id_num_per_feature) {
		int64_t* old_id = g_queue_pop_head(ie->ids);
		free(old_id);
	}
}

void init_feature_index() {
	feature_index = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
	NULL, free_feature_index_elem);

	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/feature.index");

	FILE *fp;
	if ((fp = fopen(indexpath, "r"))) {
		int feature_num;
		fread(&feature_num, sizeof(int), 1, fp);
		for (; feature_num > 0; feature_num--) {
			struct featureIndexElem * ie = new_feature_index_elem();
			fread(&ie->feature, sizeof(fingerprint), 1, fp);
			int id_num;
			fread(&id_num, sizeof(int), 1, fp);
			for (; id_num > 0; id_num--) {
				int64_t *id = (int64_t*) malloc(sizeof(int64_t));
				fread(id, sizeof(int64_t), 1, fp);
				g_queue_push_tail(ie->ids, id);
			}
		}
		fclose(fp);
	}

	sdsfree(indexpath);

	if (destor.index_segment_selection_method[0] == INDEX_SEGMENT_SELECT_ALL
			|| destor.index_segment_selection_method[0]
					== INDEX_SEGMENT_SELECT_LATEST)
		max_id_num_per_feature = 1;
	else
		max_id_num_per_feature = 5;
}

void close_feature_index() {
	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/feature.index");

	FILE *fp;
	if ((fp = fopen(indexpath, "w")) == NULL) {
		perror("Can not open index/feature.index for write because:");
		exit(1);
	}

	int feature_num = g_hash_table_size(feature_index);
	fwrite(&feature_num, sizeof(int), 1, fp);

	GHashTableIter iter;
	gpointer key;
	struct featureIndexElem* ie;

	g_hash_table_iter_init(&iter, feature_index);
	while (g_hash_table_iter_next(&iter, &key, &ie)) {
		fwrite(&ie->feature, sizeof(fingerprint), 1, fp);
		int id_num = g_queue_get_length(ie->ids);
		fwrite(&id_num, sizeof(int), 1, fp);
		int i;
		for (i = 0; i < id_num; i++) {
			fwrite(g_queue_peek_nth(ie->ids, i), sizeof(int64_t), 1, fp);
		}
	}

	fclose(fp);

	sdsfree(indexpath);

	g_hash_table_destroy(feature_index);
}

/*
 * For top-k selection method.
 */
GQueue* feature_index_lookup(fingerprint *feature) {
	struct featureIndexElem* ie = g_hash_table_lookup(feature_index, feature);
	return ie ? ie->ids : NULL;
}

/*
 * For all and latest selection method,
 * earlier segments make no sense for them.
 */
segmentid feature_index_lookup_for_latest(fingerprint *feature) {
	struct featureIndexElem* ie = g_hash_table_lookup(feature_index, feature);
	if (ie) {
		segmentid *id = g_queue_peek_tail(ie->ids);
		return *id;
	} else
		return TEMPORARY_ID;
}

void feature_index_update(fingerprint *feature, int64_t id) {
	struct featureIndexElem* ie = g_hash_table_lookup(feature_index, feature);
	if (!ie) {
		ie = new_feature_index_elem();
		memcpy(&ie->feature, feature, sizeof(fingerprint));
		g_hash_table_insert(feature_index, &ie->feature, ie);
	}
	feature_index_elem_add_id(ie, id);
}
