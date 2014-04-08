/*
 * recipemanage.c
 *
 *  Created on: May 22, 2012
 *      Author: fumin
 */

#include "recipestore.h"
#include "../jcr.h"

static int32_t backup_version_count;
static sds recipepath;
/* Used for seed */
static containerid seed_id = TEMPORARY_ID;

void init_recipe_store() {
	recipepath = sdsdup(destor.working_directory);
	recipepath = sdscat(recipepath, "/recipes/");

	sds count_fname = sdsdup(recipepath);
	count_fname = sdscat(count_fname, "backupversion.count");

	FILE *fp;
	if ((fp = fopen(count_fname, "r"))) {
		/* Read if exists. */
		fread(&backup_version_count, 4, 1, fp);
		fclose(fp);
	}

	sdsfree(count_fname);
}

void close_recipe_store() {
	sds count_fname = sdsdup(recipepath);
	count_fname = sdscat(count_fname, "backupversion.count");

	FILE *fp;
	if ((fp = fopen(count_fname, "w")) == NULL) {
		perror("Can not open recipes/backupversion.count for write");
		exit(1);
	}

	fwrite(&backup_version_count, 4, 1, fp);

	fclose(fp);
	sdsfree(count_fname);
}

int32_t get_next_version_number() {
	return backup_version_count++;
}

/*
 * Create a new backupVersion structure for a backup run.
 */
struct backupVersion* create_backup_version(const char *path) {
	struct backupVersion *b = (struct backupVersion *) malloc(
			sizeof(struct backupVersion));

	b->bv_num = get_next_version_number();
	b->path = sdsnew(path);

	/*
	 * If the path points to a file,
	 */
	int cur = sdslen(b->path) - 1;
	while (b->path[cur] != '/') {
		b->path[cur] = 0;
		cur--;
	}
	sdsupdatelen(b->path);

	b->deleted = 0;
	b->number_of_chunks = 0;
	b->number_of_files = 0;

	b->fname_prefix = sdsdup(recipepath);
	b->fname_prefix = sdscat(b->fname_prefix, "bv");
	char s[20];
	sprintf(s, "%d", b->bv_num);
	b->fname_prefix = sdscat(b->fname_prefix, s);

	sds fname = sdsdup(b->fname_prefix);
	fname = sdscat(fname, ".meta");
	if ((b->metadata_fp = fopen(fname, "w")) == 0) {
		fprintf(stderr, "Can not create bv%d.meta!\n", b->bv_num);
		exit(1);
	}

	fseek(b->metadata_fp, 0, SEEK_SET);
	fwrite(&b->bv_num, sizeof(b->bv_num), 1, b->metadata_fp);
	fwrite(&b->deleted, sizeof(b->deleted), 1, b->metadata_fp);

	fwrite(&b->number_of_files, sizeof(b->number_of_files), 1, b->metadata_fp);
	fwrite(&b->number_of_chunks, sizeof(b->number_of_chunks), 1,
			b->metadata_fp);

	int pathlen = sdslen(b->path);
	fwrite(&pathlen, sizeof(pathlen), 1, b->metadata_fp);
	fwrite(b->path, sdslen(b->path), 1, b->metadata_fp);

	fname = sdscpy(fname, b->fname_prefix);
	fname = sdscat(fname, ".recipe");
	if ((b->recipe_fp = fopen(fname, "w")) <= 0) {
		fprintf(stderr, "Can not create bv%d.recipe!\n", b->bv_num);
		exit(1);
	}

	fname = sdscpy(fname, b->fname_prefix);
	fname = sdscat(fname, ".seed");
	if ((b->seed_fp = fopen(fname, "w")) <= 0) {
		fprintf(stderr, "Can not create bv%d.seed!\n", b->bv_num);
		exit(1);
	}

	sdsfree(fname);

	return b;
}

/*
 * Check the existence of a backup.
 */
int backup_version_exists(int number) {
	sds fname = sdsdup(recipepath);
	fname = sdscat(fname, "bv");
	char s[20];
	sprintf(s, "%d", number);
	fname = sdscat(fname, s);
	fname = sdscat(fname, ".meta");

	if (access(fname, 0) == 0) {
		sdsfree(fname);
		return 1;
	}
	sdsfree(fname);
	return 0;
}

/*
 * Open an existing bversion for a restore run.
 */
struct backupVersion* open_backup_version(int number) {

	if (!backup_version_exists(number)) {
		fprintf(stderr, "Backup version %d doesn't exist", number);
		exit(1);
	}

	struct backupVersion *b = (struct backupVersion *) malloc(
			sizeof(struct backupVersion));

	b->fname_prefix = sdsdup(recipepath);
	b->fname_prefix = sdscat(b->fname_prefix, "bv");
	char s[20];
	sprintf(s, "%d", number);
	b->fname_prefix = sdscat(b->fname_prefix, s);

	sds fname = sdsdup(b->fname_prefix);
	fname = sdscat(fname, ".meta");
	if ((b->metadata_fp = fopen(fname, "r")) == 0) {
		fprintf(stderr, "Can not open bv%d.meta!\n", b->bv_num);
		exit(1);
	}

	fseek(b->metadata_fp, 0, SEEK_SET);
	fread(&b->bv_num, sizeof(b->bv_num), 1, b->metadata_fp);
	assert(b->bv_num == number);
	fread(&b->deleted, sizeof(b->deleted), 1, b->metadata_fp);

	if (b->deleted) {
		fprintf(stderr, "Backup version %d has been deleted!\n", number);
		exit(1);
	}

	fread(&b->number_of_files, sizeof(b->number_of_files), 1, b->metadata_fp);
	fread(&b->number_of_chunks, sizeof(b->number_of_chunks), 1, b->metadata_fp);

	int pathlen;
	fread(&pathlen, sizeof(int), 1, b->metadata_fp);
	char path[pathlen + 1];
	fread(path, pathlen, 1, b->metadata_fp);
	path[pathlen] = 0;
	b->path = sdsnew(path);

	fname = sdscpy(fname, b->fname_prefix);
	fname = sdscat(fname, ".recipe");
	if ((b->recipe_fp = fopen(fname, "r")) <= 0) {
		fprintf(stderr, "Can not open bv%d.recipe!\n", b->bv_num);
		exit(1);
	}

	fname = sdscpy(fname, b->fname_prefix);
	fname = sdscat(fname, ".seed");
	if ((b->seed_fp = fopen(fname, "r")) <= 0) {
		fprintf(stderr, "Can not open bv%d.seed!\n", b->bv_num);
		exit(1);
	}

	sdsfree(fname);

	return b;
}

/*
 * Update the metadata after a backup run is finished.
 */
void update_backup_version(struct backupVersion *b) {
	fseek(b->metadata_fp, 0, SEEK_SET);
	fwrite(&b->bv_num, sizeof(b->bv_num), 1, b->metadata_fp);
	fwrite(&b->deleted, sizeof(b->deleted), 1, b->metadata_fp);

	fwrite(&b->number_of_files, sizeof(b->number_of_files), 1, b->metadata_fp);
	fwrite(&b->number_of_chunks, sizeof(b->number_of_chunks), 1,
			b->metadata_fp);

	int pathlen = sdslen(b->path);
	fwrite(&pathlen, sizeof(pathlen), 1, b->metadata_fp);
	fwrite(b->path, sdslen(b->path), 1, b->metadata_fp);

	if (seed_id != TEMPORARY_ID)
		fprintf(b->seed_fp, "%ld\n", seed_id);
	/* An indication of end. */
	fprintf(b->seed_fp, "%ld\n", TEMPORARY_ID);
}

/*
 * Free backup version.
 */
void free_backup_version(struct backupVersion *b) {
	if (b->metadata_fp)
		fclose(b->metadata_fp);
	if (b->recipe_fp)
		fclose(b->recipe_fp);
	if (b->seed_fp)
		fclose(b->seed_fp);

	b->metadata_fp = b->recipe_fp = b->seed_fp = 0;
	sdsfree(b->path);
	free(b);
}

void append_recipe_meta(struct backupVersion* b, struct recipeMeta* r) {

	int len = sdslen(r->filename);
	fwrite(&len, sizeof(len), 1, b->metadata_fp);
	fwrite(r->filename, len, 1, b->metadata_fp);
	fwrite(&r->chunknum, sizeof(r->chunknum), 1, b->metadata_fp);
	fwrite(&r->filesize, sizeof(r->filesize), 1, b->metadata_fp);

	b->number_of_files++;
}

/*
 * 8-byte segment id,
 * 16-bit backup id, 32-bit off, 16-bit size
 */
static inline segmentid make_segment_id(int64_t bid, int64_t off, int64_t size){
	return (bid << 48) + (off << 16) + size;
}

static inline int64_t id_to_size(segmentid id) {
	return id & (0xffff);
}

static inline int64_t id_to_off(segmentid id) {
	return (id >> 16) & 0xffffffff;
}

static inline int64_t id_to_bnum(segmentid id) {
	return id >> 48;
}

segmentid append_segment_flag(struct backupVersion* b, int flag, int segment_size){
	assert(flag == CHUNK_SEGMENT_START || flag == CHUNK_SEGMENT_END);
	struct chunkPointer* cp = (struct chunkPointer*) malloc(sizeof(struct chunkPointer));
	// Set to a negative number for being distinguished from container ID.
	cp->id = 0 - flag;
	cp->size = segment_size;

	fseek(b->recipe_fp, 0, SEEK_END);
	int64_t off = ftell(b->recipe_fp);

	NOTICE("Filter phase: write segment at %lld offset!", off);

	fwrite(&cp->fp, sizeof(fingerprint), 1, b->recipe_fp);
	fwrite(&cp->id, sizeof(containerid), 1, b->recipe_fp);
	fwrite(&cp->size, sizeof(int32_t), 1, b->recipe_fp);

	if(flag == CHUNK_SEGMENT_END){
		return TEMPORARY_ID;
	}else
		return make_segment_id(b->bv_num, off, segment_size);
}

/*
 * return the offset
 */
void append_n_chunk_pointers(struct backupVersion* b,
		struct chunkPointer* cp, int n) {

	if (n <= 0)
		return;
	int i;
	for (i = 0; i < n; i++) {
		if (seed_id != TEMPORARY_ID && seed_id != cp[i].id)
			fprintf(b->seed_fp, "%ld\n", seed_id);
		seed_id = cp[i].id;
		assert(cp[i].id != TEMPORARY_ID);
		fwrite(&cp[i].fp, sizeof(fingerprint), 1, b->recipe_fp);
		fwrite(&cp[i].id, sizeof(containerid), 1, b->recipe_fp);
		fwrite(&cp[i].size, sizeof(int32_t), 1, b->recipe_fp);

		b->number_of_chunks++;
	}
}

struct recipeMeta* read_next_recipe_meta(struct backupVersion* b) {

	static int read_file_num;

	assert(read_file_num <= b->number_of_files);

	int len;
	fread(&len, sizeof(len), 1, b->metadata_fp);
	char filename[len + 1];

	fread(filename, len, 1, b->metadata_fp);
	filename[len] = 0;

	struct recipeMeta* r = new_recipe_meta(filename);

	fread(&r->chunknum, sizeof(r->chunknum), 1, b->metadata_fp);
	fread(&r->filesize, sizeof(r->filesize), 1, b->metadata_fp);

	read_file_num++;

	return r;
}

/*
 * If return value is not NULL, a new file starts.
 * If no recipe and chunkpointer are read,
 * we arrive at the end of the stream.
 */
struct chunkPointer* read_next_n_chunk_pointers(struct backupVersion* b, int n,
		int *k) {

	/* Total number of read chunks. */
	static int read_chunk_num;

	if (read_chunk_num == b->number_of_chunks) {
		/* It's the stream end. */
		*k = 0;
		return NULL;
	}

	int num = (b->number_of_chunks - read_chunk_num) > n ?
					n : (b->number_of_chunks - read_chunk_num), i;

	struct chunkPointer *cp = (struct chunkPointer *) malloc(
			sizeof(struct chunkPointer) * num);

	for (i = 0; i < num; i++) {
		fread(&(cp[i].fp), sizeof(fingerprint), 1, b->recipe_fp);
		fread(&(cp[i].id), sizeof(containerid), 1, b->recipe_fp);
		fread(&(cp[i].size), sizeof(int32_t), 1, b->recipe_fp);
		/* Ignore segment boundaries */
		if(cp[i].id == 0 - CHUNK_SEGMENT_START || cp[i].id == 0 - CHUNK_SEGMENT_END)
			i--;

	}

	*k = num;

	read_chunk_num += num;
	assert(read_chunk_num <= b->number_of_chunks);

	return cp;
}

containerid* read_next_n_seeds(struct backupVersion* b, int n, int *k) {
	static int end;

	if (end) {
		*k = 0;
		return NULL;
	}

	containerid *ids = (containerid *) malloc(sizeof(containerid) * (n + 1));
	int i;
	for (i = 1; i < n + 1; i++) {
		fscanf(b->seed_fp, "%ld", &ids[i]);
		if (ids[i] == TEMPORARY_ID) {
			end = 1;
			break;
		}
	}

	*k = i - 1;
	ids[0] = *k;
	return ids;
}

struct recipeMeta* new_recipe_meta(char* name) {
	struct recipeMeta* r = (struct recipeMeta*) malloc(sizeof(struct recipeMeta));
	r->filename = sdsnew(name);
	r->chunknum = 0;
	r->filesize = 0;
	return r;
}

void free_recipe_meta(struct recipeMeta* r) {
	sdsfree(r->filename);
	free(r);
}

struct segmentRecipe* new_segment_recipe() {
	struct segmentRecipe* sr = (struct segmentRecipe*) malloc(
			sizeof(struct segmentRecipe));
	sr->id = TEMPORARY_ID;
	sr->kvpairs = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
			NULL, free);
	return sr;
}

void free_segment_recipe(struct segmentRecipe* sr) {
	g_hash_table_destroy(sr->kvpairs);
	free(sr);
}

GQueue* prefetch_segments(segmentid id, int prefetch_num) {
	static struct backupVersion *opened_bv;

	if (id == TEMPORARY_ID) {
		assert(id != TEMPORARY_ID);
		return NULL;
	}

	int64_t bnum = id_to_bnum(id);
	int64_t off = id_to_off(id);
	int64_t size = id_to_size(id);

	/* All prefetched segment recipes */
	GQueue *segments = g_queue_new();

	if(opened_bv == NULL || opened_bv->bv_num != bnum){

		if(opened_bv && opened_bv->bv_num != jcr.bv->bv_num)
			free_backup_version(opened_bv);

		if(bnum == jcr.bv->bv_num)
			opened_bv = jcr.bv;
		else{
			opened_bv = open_backup_version(bnum);
			assert(opened_bv);
		}
	}

	fseek(opened_bv->recipe_fp, off, SEEK_SET);

	NOTICE("Dedup phase: Read segment %lld in backup %lld of %lld offset and %lld size",
			id, bnum, off, size);

	struct chunkPointer flag;
	int j;
	for (j = 0; j < prefetch_num; j++) {

		int64_t current_off = ftell(opened_bv->recipe_fp);

		fread(&flag.fp, sizeof(flag.fp), 1, opened_bv->recipe_fp);
		fread(&flag.id, sizeof(flag.id), 1, opened_bv->recipe_fp);
		fread(&flag.size, sizeof(flag.size), 1, opened_bv->recipe_fp);
		if(flag.id != -CHUNK_SEGMENT_START){
			assert(j!=0);
			NOTICE("Dedup phase: no more segment can be prefetched!");
			break;
		}

		struct segmentRecipe* sr = new_segment_recipe();
		sr->id = make_segment_id(opened_bv->bv_num, current_off, flag.size);

		int i;
		for (i = 0; i < flag.size; i++) {
			struct chunkPointer* cp = (struct chunkPointer*) malloc(
					sizeof(struct chunkPointer));
			fread(&cp->fp, sizeof(cp->fp), 1, opened_bv->recipe_fp);
			fread(&cp->id, sizeof(cp->id), 1, opened_bv->recipe_fp);
			fread(&cp->size, sizeof(cp->size), 1, opened_bv->recipe_fp);
			g_hash_table_replace(sr->kvpairs, &cp->fp, cp);
		}

		fread(&flag.fp, sizeof(flag.fp), 1, opened_bv->recipe_fp);
		fread(&flag.id, sizeof(flag.id), 1, opened_bv->recipe_fp);
		fread(&flag.size, sizeof(flag.size), 1, opened_bv->recipe_fp);
		assert(flag.id == 0 - CHUNK_SEGMENT_END);

		g_queue_push_tail(segments, sr);
	}

	return segments;
}

int segment_recipe_check_id(struct segmentRecipe* sr, segmentid *id) {
     return sr->id == *id;
}

int lookup_fingerprint_in_segment_recipe(struct segmentRecipe* sr,
        fingerprint *fp) {
    return g_hash_table_lookup(sr->kvpairs, fp) == NULL ? 0 : 1;
}
