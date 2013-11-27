/*
 * recipemanage.c
 *
 *  Created on: May 22, 2012
 *      Author: fumin
 */

#include "recipemanage.h"

static int32_t backup_version_count = 0;
static sds recipepath;

void init_recipe_management() {
	recipepath = sdsdup(destor.working_directory);
	recipepath = sdscat(recipepath, "/recipes/");

	sds count_fname = sdsdup(recipepath);
	count_fname = sdscat(count_fname, "backupversion.count");

	FILE *fp;
	if ((fp = fopen(count_fname, "rw+")) == NULL) {
		fprintf(stderr, "Can not open recipes/backupversion.count for read\n");
		exit(1);
	}

	int32_t count;
	if (fread(&count, 4, 1, fp) == 1) {
		backup_version_count = count;
	}

	sdsfree(count_fname);
	fclose(fp);
}

void close_recipe_management() {
	sds count_fname = sdsdup(recipepath);
	count_fname = sdscat(count_fname, "backupversion.count");

	FILE *fp;
	if ((fp = fopen(count_fname, "w+")) == NULL) {
		fprintf(stderr, "Can not open recipes/backupversion.count for write\n");
		exit(1);
	}

	fwrite(&backup_version_count, 4, 1, fp);

	fclose(fp);
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

	b->fname_prefix = sdsdup(recipepath);
	b->fname_prefix = sdscat(b->fname_prefix, "bv");
	b->fname_prefix = sdscat(b->fname_prefix, itoa(b->number));

	sds fname = sdsdup(b->fname_prefix);
	fname = sdscat(fname, ".meta");
	if ((b->metadata_fp = fopen(fname, "w+")) == 0) {
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
	if ((b->recipe_fp = fopen(fname, "w+")) <= 0) {
		fprintf(stderr, "Can not create bv%d.recipe!\n", b->number);
		exit(1);
	}

	fname = sdscpy(fname, b->fname_prefix);
	fname = sdscat(fname, ".seed");
	if ((b->seed_fp = fopen(fname, "w+")) <= 0) {
		fprintf(stderr, "Can not create bv%d.seed!\n", b->number);
		exit(1);
	}

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
	fname = sdscat(fname, itoa(number));
	fname = sdscat(fname, ".meta");

	if (access(fname, 0) == 0)
		return 1;

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

	b->number = number;

	b->fname_prefix = sdsdup(recipepath);
	b->fname_prefix = sdscat(b->fname_prefix, "bv");
	b->fname_prefix = sdscat(b->fname_prefix, itoa(b->number));

	sds fname = sdsdup(b->fname_prefix);
	fname = sdscat(fname, ".meta");
	if ((b->metadata_fp = fopen(fname, "r")) == 0) {
		fprintf(stderr, "Can not open bv%d.meta!\n", b->number);
		exit(1);
	}

	fseek(b->metadata_fp, 0, SEEK_SET);
	fread(&b->number, sizeof(b->number), 1, b->metadata_fp);
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
	fread(path, sizeof(pathlen), 1, b->metadata_fp);
	path[pathlen] = 0;
	b->path = sdsdup(path);

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

	/*set deleted until backup is completed.*/
	b->deleted = 1;
	b->number_of_chunks = 0;
	b->number_of_files = 0;

	return b;
}

/*
 * Update the metadata after a backup run is finished.
 */
void update_backup_version(struct backupVersion *b) {
	fseek(b->metadata_fp, 0, SEEK_SET);
	fwrite(&b->number, sizeof(b->number), 1, b->metadata_fp);
	fwrite(&b->deleted, sizeof(b->deleted), 1, b->metadata_fp);

	fwrite(&b->number_of_files, sizeof(b->number_of_files), 1, b->metadata_fp);
	fwrite(&b->number_of_chunks, sizeof(b->number_of_chunks), 1,
			b->metadata_fp);

	int pathlen = sdslen(b->path);
	fwrite(&pathlen, sizeof(pathlen), 1, b->metadata_fp);
	fwrite(b->path, sdslen(b->path), 1, b->metadata_fp);
}

/*
 * Free bversion.
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
}

void append_n_chunk_pointers(struct backupVersion* b, struct chunkPointer* cp,
		int n) {
	if (n <= 0)
		return;
	int i;
	for (i = 0; i < n; i++) {
		fwrite(&cp[i].fp, sizeof(fingerprint), 1, b->recipe_fp);
		fwrite(&cp[i].id, sizeof(containerid), 1, b->recipe_fp);
	}
}

struct recipe* read_next_recipe_meta(struct backupVersion* b) {
	struct recipe* r = (struct recipe*) malloc(sizeof(struct recipe));
	int len;
	fread(&len, sizeof(len), 1, b->metadata_fp);
	char filename[len + 1];

	fread(filename, len, 1, b->metadata_fp);
	filename[len] = 0;
	r->filename = sdsnew(filename);

	fread(&r->chunknum, sizeof(r->chunknum), 1, b->metadata_fp);
	fread(&r->filesize, sizeof(r->filesize), 1, b->metadata_fp);

	return r;
}

/*
 * If return value is not NULL, a new file starts.
 */
struct recipe* read_next_n_chunk_pointers(struct backupVersion* b, int n,
		struct chunkPointer** cp, int *k) {
	struct recipe *r = NULL;
	static int number_of_remaining_chunks = 0;
	if (number_of_remaining_chunks == 0) {
		r = read_next_recipe_meta(b);
		number_of_remaining_chunks = b->number_of_chunks;
		return r;
	}

	*cp = (struct chunkpointer *) malloc(sizeof(struct chunkpointer) * n);

	*k = 0;
	while (*k < n) {
		if (number_of_remaining_chunks == 0)
			break;
		fread((*cp)[0].fp, sizeof(fingerprint), 1, b->recipe_fp);
		fread((*cp)[0].id, sizeof(containerid), 1, b->recipe_fp);
		number_of_remaining_chunks--;
		(*k)++;
	}

	return NULL;
}

void append_seed(struct backupVersion* b, containerid id, int32_t size) {
	fprintf(b->seed_fp, "%d %d\n", id, size);
}
