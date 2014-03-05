/*
 * feature_index.c
 *
 *  Created on: Nov 21, 2013
 *      Author: fumin
 */

#include "feature_index.h"

struct featureIndexElem {
	fingerprint feature;
	/* segmentid or containerid */
	int64_t ids[0];
};

static struct featureIndexElem* new_feature_index_elem() {
	struct featureIndexElem* elem = (struct featureIndexElem*) malloc(
			sizeof(struct featureIndexElem)
					+ destor.index_feature_segment_num * sizeof(int64_t));

	int i;
	for (i = 0; i < destor.index_feature_segment_num; i++) {
		elem->ids[i] = TEMPORARY_ID;
	}

	return elem;
}

static void feature_index_elem_add_id(struct featureIndexElem* e, int64_t id) {
	assert(id != TEMPORARY_ID);
	memmove(e->ids, &e->ids[1],
			(destor.index_feature_segment_num - 1) * sizeof(int64_t));
	e->ids[0] = id;
}

static GHashTable *feature_index;

void init_feature_index() {

	if (destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY
			|| destor.index_segment_selection_method[0]
					== INDEX_SEGMENT_SELECT_LAZY)
		destor.index_feature_segment_num = 1;

	feature_index = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
	NULL, free);

	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/feature.index");

	/* Initialize the feature index from the dump file. */
	FILE *fp;
	if ((fp = fopen(indexpath, "r"))) {
		/* The number of features */
		int feature_num;
		fread(&feature_num, sizeof(int), 1, fp);
		for (; feature_num > 0; feature_num--) {
			/* Read a feature */
			struct featureIndexElem * ie = new_feature_index_elem();
			fread(&ie->feature, sizeof(fingerprint), 1, fp);

			/* The number of segments/containers the feature refers to. */
			int id_num, i;
			fread(&id_num, sizeof(int), 1, fp);
			assert(id_num <= destor.index_feature_segment_num);

			for (i = 0; i < id_num; i++)
				/* Read an ID */
				fread(&ie->ids[i], sizeof(int64_t), 1, fp);

			g_hash_table_insert(feature_index, &ie->feature, ie);
		}
		fclose(fp);
	}

	sdsfree(indexpath);
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
	gpointer key, value;
	g_hash_table_iter_init(&iter, feature_index);
	while (g_hash_table_iter_next(&iter, &key, &value)) {

		/* Write a feature. */
		struct featureIndexElem* ie = (struct featureIndexElem*) value;
		fwrite(&ie->feature, sizeof(fingerprint), 1, fp);

		/* Write the number of segments/containers */
		fwrite(&destor.index_feature_segment_num, sizeof(int), 1, fp);
		int i;
		for (i = 0; i < destor.index_feature_segment_num; i++)
			fwrite(&ie->ids[i], sizeof(int64_t), 1, fp);

	}

	destor.index_memory_footprint = g_hash_table_size(feature_index)
			* (20 + 8 * destor.index_feature_segment_num);

	fclose(fp);

	sdsfree(indexpath);

	g_hash_table_destroy(feature_index);
}

/*
 * For top-k selection method.
 */
int64_t* feature_index_lookup(fingerprint *feature) {
	struct featureIndexElem* ie = g_hash_table_lookup(feature_index, feature);
	return ie ? ie->ids : NULL;
}

/*
 * For all and latest selection method,
 * earlier segments make no sense for them.
 */
int64_t feature_index_lookup_for_latest(fingerprint *feature) {
	struct featureIndexElem* ie = g_hash_table_lookup(feature_index, feature);
	if (ie)
		return ie->ids[0];
	else
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
