/*
 * segmentstore.c
 *
 *  Created on: Nov 24, 2013
 *      Author: fumin
 */

#include "segmentstore.h"
#include "../tools/serial.h"

#define VOLUME_HEAD 20

static pthread_mutex_t mutex;

static struct {
	FILE *fp;
	int64_t segment_num;
	int64_t current_length;
} segment_volume;

static inline int64_t id_to_offset(segmentid id) {
	int64_t off = id >> 24;
	assert(off >= 0);
	return off;
}

static inline int64_t id_to_length(segmentid id) {
	return id & (0xffffff);
}

static inline segmentid make_segment_id(int64_t offset, int64_t length) {
	return (offset << 24) + length;
}

void init_segment_management() {

	sds fname = sdsnew(destor.working_directory);
	fname = sdscat(fname, "index/segment.volume");

	int32_t flag = -1;
	if ((segment_volume.fp = fopen(fname, "r+"))) {
		/* exist */
		fread(&flag, sizeof(flag), 1, segment_volume.fp);
		assert(flag == 0xff00ff00);
		/* Invalid */
		fread(&segment_volume.segment_num, sizeof(segment_volume.segment_num),
				1, segment_volume.fp);
		fread(&segment_volume.current_length,
				sizeof(segment_volume.current_length), 1, segment_volume.fp);
		sdsfree(fname);
		return;
	}

	destor_log(DESTOR_NOTICE, "Create index/segment.volume.");

	if (!(segment_volume.fp = fopen(fname, "w+"))) {

		perror("Can not create index/segment.volume because");
		exit(1);
	}

	segment_volume.current_length = 4 + 8 + 8;
	segment_volume.segment_num = 0;

	flag = 0xff00ff00;
	fwrite(&flag, sizeof(flag), 1, segment_volume.fp);
	fwrite(&segment_volume.segment_num, sizeof(segment_volume.segment_num), 1,
			segment_volume.fp);
	fwrite(&segment_volume.current_length,
			sizeof(segment_volume.current_length), 1, segment_volume.fp);

	sdsfree(fname);

	pthread_mutex_init(&mutex, NULL);
}

void close_segment_management() {

	fseek(segment_volume.fp, 0, SEEK_SET);
	int32_t flag = 0xff00ff00;
	fwrite(&flag, sizeof(flag), 1, segment_volume.fp);
	fwrite(&segment_volume.segment_num, sizeof(segment_volume.segment_num), 1,
			segment_volume.fp);
	fwrite(&segment_volume.current_length,
			sizeof(segment_volume.current_length), 1, segment_volume.fp);
	fclose(segment_volume.fp);

	pthread_mutex_destroy(&mutex);
}

struct segmentRecipe* new_segment_recipe() {
	struct segmentRecipe* sr = (struct segmentRecipe*) malloc(
			sizeof(struct segmentRecipe));
	sr->id = TEMPORARY_ID;
	sr->kvpairs = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
			NULL, free);
	sr->features = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, free, NULL);
	sr->reference_count = 1;
	pthread_mutex_init(&sr->mutex, NULL);
	return sr;
}

/* Transform a segment into a segmentRecipe */
struct segmentRecipe* new_segment_recipe_full(struct segment* s) {
	struct segmentRecipe* sr = (struct segmentRecipe*) malloc(
			sizeof(struct segmentRecipe));
	sr->id = TEMPORARY_ID;
	sr->kvpairs = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
			NULL, free);
	sr->reference_count = 1;
	pthread_mutex_init(&sr->mutex, NULL);

	int len = g_queue_get_length(s->chunks), i;
	for(i=0; i<len; i++){
		struct chunk* c = g_queue_peek_nth(s->chunks, i);

		if(CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
			continue;

		struct indexElem* elem = (struct indexElem*) malloc(
				sizeof(struct indexElem));
		memcpy(&elem->fp, &c->fp, sizeof(fingerprint));
		elem->id = c->id;
		g_hash_table_replace(sr->kvpairs, &elem->fp, elem);
	}
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
	g_hash_table_destroy(sr->kvpairs);
	pthread_mutex_destroy(&sr->mutex);
	free(sr);
}

int lookup_fingerprint_in_segment_recipe(struct segmentRecipe* sr,
		fingerprint *fp) {
	return g_hash_table_lookup(sr->kvpairs, fp) == NULL ? 0 : 1;
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
	g_hash_table_iter_init(&iter, sr->kvpairs);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct indexElem* elem = (struct indexElem*) malloc(
				sizeof(struct indexElem));
		memcpy(elem, value, sizeof(struct indexElem));
		g_hash_table_insert(dup->kvpairs, &elem->fp, elem);
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

	GHashTableIter iter;
	gpointer key, value;
	/* Iterate fingerprints in delta */
	g_hash_table_iter_init(&iter, delta->kvpairs);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct indexElem* base_elem = g_hash_table_lookup(base->kvpairs, key);
		if (!base_elem) {
			base_elem = (struct indexElem*) malloc(sizeof(struct indexElem));
			memcpy(base_elem, value, sizeof(struct indexElem));
			g_hash_table_insert(base->kvpairs, &base_elem->fp, base_elem);
		} else {
			/* Select the newer id. More discussions are required. */
			struct indexElem* ie = (struct indexElem*) value;
			base_elem->id = base_elem->id >= ie->id ? base_elem->id : ie->id;
		}
	}

	return base;
}

struct segmentRecipe* retrieve_segment(segmentid id) {
	if (id == TEMPORARY_ID)
		return NULL;

	int64_t offset = id_to_offset(id);
	int64_t length = id_to_length(id);

	pthread_mutex_lock(&mutex);

	char buf[length];
	fseek(segment_volume.fp, offset, SEEK_SET);
	if (fread(buf, length, 1, segment_volume.fp) != 1) {
		WARNING("Dedup phase: Prefetch an unready segment of %lld offset",
				offset);
		exit(1);
	}

	pthread_mutex_unlock(&mutex);

	VERBOSE("Dedup phase: Read similar segment of %lld offset", offset);

	struct segmentRecipe* sr = new_segment_recipe();

	unser_declare;
	unser_begin(buf, length);
	unser_bytes(&sr->id, sizeof(segmentid));
	assert(sr->id == id);

	int num, i;
	unser_int32(num);
	for (i = 0; i < num; i++) {
		struct indexElem* e = (struct indexElem*) malloc(
				sizeof(struct indexElem));
		unser_int64(e->id);
		unser_bytes(&e->fp, sizeof(fingerprint));
		g_hash_table_insert(sr->kvpairs, &e->fp, e);
	}

	unser_end(buf, length);

	return sr;
}

GQueue* prefetch_segments(segmentid id, int prefetch_num) {
	if (id == TEMPORARY_ID) {
		assert(id != TEMPORARY_ID);
		return NULL;
	}

	/* All prefetched segment recipes */
	GQueue *segments = g_queue_new();

	int64_t offset = id_to_offset(id);

	pthread_mutex_lock(&mutex);

	fseek(segment_volume.fp, offset, SEEK_SET);

	VERBOSE("Dedup phase: Read similar segment %lld of %lld offset and %lld length", id, offset,
			id_to_length(id));

	int j;
	for (j = 0; j < prefetch_num; j++) {
		segmentid rid;
		if (fread(&rid, sizeof(rid), 1, segment_volume.fp) != 1) {
			NOTICE("Dedup phase: no more segments can be prefetched");
			pthread_mutex_unlock(&mutex);
			return segments;
		}
		int64_t length = id_to_length(rid);
		VERBOSE("Dedup phase: Prefetch %dth segment of %lld offset and %lld length",
				j, id_to_offset(rid), length);

		char buf[length];
		if (fread(buf, length - sizeof(rid), 1, segment_volume.fp) != 1) {
			WARNING("Dedup phase: Prefetch an unready segment of %lld offset",
					id_to_offset(rid));
			exit(1);
		}

		struct segmentRecipe* sr = new_segment_recipe();
		sr->id = rid;

		unser_declare;
		unser_begin(buf, length);

		int num, i;
		unser_int32(num);
		for (i = 0; i < num; i++) {
			struct indexElem* e = (struct indexElem*) malloc(
					sizeof(struct indexElem));
			unser_int64(e->id);
			unser_bytes(&e->fp, sizeof(fingerprint));
			g_hash_table_insert(sr->kvpairs, &e->fp, e);
		}

		unser_end(buf, length);

		g_queue_push_tail(segments, sr);
	}

	pthread_mutex_unlock(&mutex);

	return segments;
}

struct segmentRecipe* update_segment(struct segmentRecipe* sr) {
	int64_t offset = segment_volume.current_length;
	int64_t length = 8 + 4 + g_hash_table_size(sr->kvpairs)
					* (sizeof(fingerprint) + sizeof(containerid));
	sr->id = make_segment_id(offset, length);

	VERBOSE("Filter phase: write %lld segment of %lld offset, %d fps",
			sr->id, id_to_offset(sr->id), g_hash_table_size(sr->kvpairs));

	char buf[length];
	ser_declare;
	ser_begin(buf, length);

	ser_bytes(&sr->id, sizeof(segmentid));

	int num = g_hash_table_size(sr->kvpairs);
	ser_int32(num);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, sr->kvpairs);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct indexElem* e = (struct indexElem*) value;
		ser_int64(e->id);
		ser_bytes(&e->fp, sizeof(fingerprint));
	}
	ser_end(buf, length);

	pthread_mutex_lock(&mutex);

	fseek(segment_volume.fp, offset, SEEK_SET);
	fwrite(buf, length, 1, segment_volume.fp);

	pthread_mutex_unlock(&mutex);

	segment_volume.current_length += length;
	segment_volume.segment_num++;
	return sr;
}
