/*
 * recipemanage.c
 *
 *  Created on: May 22, 2012
 *      Author: fumin
 */

#include "recipestore.h"

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

	b->number = get_next_version_number();
	b->path = sdsnew(path);
	b->deleted = 1;
	b->number_of_chunks = 0;
	b->number_of_files = 0;

	b->fname_prefix = sdsdup(recipepath);
	b->fname_prefix = sdscat(b->fname_prefix, "bv");
	char s[20];
	sprintf(s, "%d", b->number);
	b->fname_prefix = sdscat(b->fname_prefix, s);

	sds fname = sdsdup(b->fname_prefix);
	fname = sdscat(fname, ".meta");
	if ((b->metadata_fp = fopen(fname, "w")) == 0) {
		fprintf(stderr, "Can not create bv%d.meta!\n", b->number);
		exit(1);
	}

	fseek(b->metadata_fp, 0, SEEK_SET);
	fwrite(&b->number, sizeof(b->number), 1, b->metadata_fp);
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
		fprintf(stderr, "Can not create bv%d.recipe!\n", b->number);
		exit(1);
	}

	fname = sdscpy(fname, b->fname_prefix);
	fname = sdscat(fname, ".seed");
	if ((b->seed_fp = fopen(fname, "w")) <= 0) {
		fprintf(stderr, "Can not create bv%d.seed!\n", b->number);
		exit(1);
	}

	sdsfree(fname);

	/*set deleted until backup is completed.*/
	b->deleted = 1;
	b->number_of_chunks = 0;
	b->number_of_files = 0;

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
		fprintf(stderr, "Can not open bv%d.meta!\n", b->number);
		exit(1);
	}

	fseek(b->metadata_fp, 0, SEEK_SET);
	fread(&b->number, sizeof(b->number), 1, b->metadata_fp);
	assert(b->number == number);
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
		fprintf(stderr, "Can not open bv%d.recipe!\n", b->number);
		exit(1);
	}

	fname = sdscpy(fname, b->fname_prefix);
	fname = sdscat(fname, ".seed");
	if ((b->seed_fp = fopen(fname, "r")) <= 0) {
		fprintf(stderr, "Can not open bv%d.seed!\n", b->number);
		exit(1);
	}

	sdsfree(fname);

	return b;
}

/*
 * Update the metadata after a backup run is finished.
 */
void update_backup_version(struct backupVersion *b) {
	b->deleted = 0;

	fseek(b->metadata_fp, 0, SEEK_SET);
	fwrite(&b->number, sizeof(b->number), 1, b->metadata_fp);
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

void append_recipe_meta(struct backupVersion* b, struct recipe* r) {

	int len = sdslen(r->filename);
	fwrite(&len, sizeof(len), 1, b->metadata_fp);
	fwrite(r->filename, len, 1, b->metadata_fp);
	fwrite(&r->chunknum, sizeof(r->chunknum), 1, b->metadata_fp);
	fwrite(&r->filesize, sizeof(r->filesize), 1, b->metadata_fp);

	b->number_of_files++;
}

void append_n_chunk_pointers(struct backupVersion* b, struct chunkPointer* cp,
		int n) {

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

struct recipe* read_next_recipe_meta(struct backupVersion* b) {

	static int read_file_num;

	assert(read_file_num <= b->number_of_files);

	int len;
	fread(&len, sizeof(len), 1, b->metadata_fp);
	char filename[len + 1];

	fread(filename, len, 1, b->metadata_fp);
	filename[len] = 0;

	struct recipe* r = new_recipe(filename);

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

	int num =
			(b->number_of_chunks - read_chunk_num) > n ?
					n : (b->number_of_chunks - read_chunk_num);

	struct chunkPointer *cp = (struct chunkPointer *) malloc(
			sizeof(struct chunkPointer) * num);

	int i;
	for (i = 0; i < num; i++) {
		fread(&(cp[i].fp), sizeof(fingerprint), 1, b->recipe_fp);
		fread(&(cp[i].id), sizeof(containerid), 1, b->recipe_fp);
		fread(&(cp[i].size), sizeof(int32_t), 1, b->recipe_fp);

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

struct recipe* new_recipe(char* name) {
	struct recipe* r = (struct recipe*) malloc(sizeof(struct recipe));
	r->filename = sdsnew(name);
	r->chunknum = 0;
	r->filesize = 0;
	return r;
}

void free_recipe(struct recipe* r) {
	sdsfree(r->filename);
	free(r);
}
