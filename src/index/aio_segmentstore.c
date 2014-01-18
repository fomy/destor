/*
 * aio_segmentstore.c
 *
 *  Created on: Nov 23, 2013
 *      Author: fumin
 */

/*
 * All-in-one segment management used by Extreme Binning.
 */

#include "aio_segmentstore.h"
#include "../tools/serial.h"

#define LEVEL_NUM 20
#define LEVEL_FACTOR 512
#define VOLUME_HEAD_SIZE 16
#define SEGMENT_HEAD_SIZE 24

static pthread_mutex_t mutex;

struct segmentVolume {
	int32_t level;
	int32_t current_segment_num;
	int64_t current_volume_length;
	sds fname;
	FILE *fp;
};

static struct segmentVolume *segment_volume_array[LEVEL_NUM];

static struct segmentVolume* init_segment_volume(int32_t level) {
	struct segmentVolume* sv = (struct segmentVolume*) malloc(
			sizeof(struct segmentVolume));
	sv->level = level;
	sv->current_segment_num = 0;
	sv->current_volume_length = VOLUME_HEAD_SIZE;
	sv->fp = NULL;

	sv->fname = sdsnew(destor.working_directory);
	sv->fname = sdscat(sv->fname, "index/segment.volume");
	char s[20];
	sprintf(s, "%d", level);
	sv->fname = sdscat(sv->fname, s);

	FILE *fp;
	if ((fp = fopen(sv->fname, "r"))) {
		int32_t tmp = -1;
		fread(&tmp, sizeof(level), 1, fp);
		assert(tmp == level);
		fread(&sv->current_segment_num, sizeof(sv->current_segment_num), 1, fp);
		fread(&sv->current_volume_length, sizeof(sv->current_volume_length), 1,
				fp);

		fclose(fp);
		return sv;
	}

	destor_log(DESTOR_NOTICE, "Create segment volume %d", level);
	if (!(fp = fopen(sv->fname, "w"))) {
		perror("Can not create index/segment.volume because");
		exit(1);
	}

	fwrite(&level, sizeof(level), 1, fp);
	fwrite(&sv->current_segment_num, sizeof(sv->current_segment_num), 1, fp);
	fwrite(&sv->current_volume_length, sizeof(sv->current_volume_length), 1,
			fp);

	fclose(fp);

	return sv;
}

static void close_segment_volume(struct segmentVolume *sv) {
	if (!sv)
		return;

	if (sv->fp == NULL) {
		if ((sv->fp = fopen(sv->fname, "r+")) == NULL) {
			destor_log(DESTOR_WARNING, "Failed to open segment volume %d\n",
					sv->level);
			perror("The reason is");
			exit(1);
		}
	}
	fseek(sv->fp, 0, SEEK_SET);
	fwrite(&sv->level, sizeof(sv->level), 1, sv->fp);
	fwrite(&sv->current_segment_num, sizeof(sv->current_segment_num), 1,
			sv->fp);
	fwrite(&sv->current_volume_length, sizeof(sv->current_volume_length), 1,
			sv->fp);
	fclose(sv->fp);

	sdsfree(sv->fname);
	free(sv);
}

static int32_t no_to_level(int32_t feature_num, int32_t chunk_num) {
	/* segment id + feature num + features + chunk num + chunks */
	int32_t size = 8 + 4 + 20 * feature_num + 4 + chunk_num * 28;
	size /= LEVEL_FACTOR;
	int level = 0;
	while (size) {
		++level;
		size >>= 1;
	}
	assert(level < LEVEL_NUM);
	return level;
}

static int32_t level_to_max_size(int32_t level) {
	if (level < 0 || level >= LEVEL_NUM) {
		destor_log(DESTOR_WARNING, "Invalid level %d\n", level);
		return -1;
	}
	int32_t size = (1 << level) * LEVEL_FACTOR;
	return size;
}

void init_aio_segment_management() {
	int i = 0;
	for (; i < LEVEL_NUM; ++i) {
		segment_volume_array[i] = init_segment_volume((int64_t) i);
	}
	pthread_mutex_init(&mutex, NULL);
}

void close_aio_segment_management() {
	int i = 0;
	for (; i < LEVEL_NUM; ++i) {
		close_segment_volume(segment_volume_array[i]);
	}

	pthread_mutex_destroy(&mutex);

}

static inline int32_t id_to_level(segmentid id) {
	return id >> 56;
}

static inline int64_t id_to_offset(segmentid id) {
	return id & (0xffffffffffffff);
}

static inline segmentid make_segment_id(int64_t level, int64_t offset) {
	return (level << 56) + offset;
}

struct segmentRecipe* retrieve_segment_all_in_one(segmentid id) {
	if (id == TEMPORARY_ID)
		return NULL;

	int32_t level = id_to_level(id);
	assert(level < LEVEL_NUM);
	int64_t offset = id_to_offset(id);

	VERBOSE("Dedup phase: Read similar segment of level %lld and offset %lld",
			level, offset);

	struct segmentVolume* sv = segment_volume_array[level];

	if (sv->fp == NULL)
		sv->fp = fopen(sv->fname, "r+");

	int32_t size = level_to_max_size(level);
	char buf[size];

	pthread_mutex_lock(&mutex);

	fseek(sv->fp, offset, SEEK_SET);
	fread(buf, size, 1, sv->fp);

	pthread_mutex_unlock(&mutex);

	struct segmentRecipe* sr = new_segment_recipe();

	unser_declare;
	unser_begin(buf, size);

	unser_int64(sr->id);
	assert(sr->id == id);
	int num, i;
	unser_int32(num);
	for (i = 0; i < num; i++) {
		fingerprint *feature = (fingerprint*) malloc(sizeof(fingerprint));
		unser_bytes(feature, sizeof(fingerprint));
		g_hash_table_insert(sr->features, feature, feature);
	}

	unser_int32(num);
	for (i = 0; i < num; i++) {
		struct indexElem* ie = (struct indexElem*) malloc(
				sizeof(struct indexElem));
		unser_int64(ie->id);
		unser_bytes(&ie->fp, sizeof(fingerprint));
		g_hash_table_insert(sr->index, &ie->fp, ie);
	}

	unser_end(buf, size);

	return sr;
}

struct segmentRecipe* update_segment_all_in_one(struct segmentRecipe* sr) {

	int level = no_to_level(g_hash_table_size(sr->features),
			g_hash_table_size(sr->index));
	struct segmentVolume* sv = segment_volume_array[level];
	int64_t offset;

	if (sr->id == TEMPORARY_ID) {
		/* New segment */
		offset = sv->current_volume_length;
		sr->id = make_segment_id(level, offset);

		sv->current_segment_num++;
		sv->current_volume_length += level_to_max_size(level);

		VERBOSE("Dedup phase: New segment of level %lld and offset %lld", level,
				offset);
	} else if (id_to_level(sr->id) != level) {
		assert(id_to_level(sr->id) < level);
		/* Migrate the segment */
		offset = sv->current_volume_length;
		sr->id = make_segment_id(level, offset);

		struct segmentVolume* old_sv = segment_volume_array[id_to_level(sr->id)];
		old_sv->current_segment_num--;

		VERBOSE("Dedup phase: Migrate a segment of level %lld to level %lld",
				old_sv->level, level);

		sv->current_segment_num++;
		sv->current_volume_length += level_to_max_size(level);
	} else {
		/* Update the segment in-place. */
		offset = id_to_offset(sr->id);

		VERBOSE(
				"Dedup phase: In-place update segment of level %lld and offset %lld",
				level, offset);
	}

	char buf[level_to_max_size(level)];
	ser_declare;
	ser_begin(buf, level_to_max_size(level));

	ser_int64(sr->id);
	int32_t num = g_hash_table_size(sr->features);
	ser_int32(num);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, sr->features);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		ser_bytes(key, sizeof(fingerprint));
	}

	num = g_hash_table_size(sr->index);
	ser_int32(num);

	g_hash_table_iter_init(&iter, sr->index);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct indexElem* e = (struct indexElem*) value;
		ser_int64(e->id);
		ser_bytes(&e->fp, sizeof(fingerprint));
	}

	ser_end(buf, level_to_max_size(level));

	if (!sv->fp)
		sv->fp = fopen(sv->fname, "r+");

	pthread_mutex_lock(&mutex);

	fseek(sv->fp, offset, SEEK_SET);
	fwrite(buf, level_to_max_size(level), 1, sv->fp);

	pthread_mutex_unlock(&mutex);

	return sr;
}
