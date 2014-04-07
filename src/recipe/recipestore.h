/*
 * recipestore.h
 *
 *  Created on: May 22, 2012
 *      Author: fumin
 */

#ifndef RECIPESTORE_H_
#define RECIPESTORE_H_

#include "../destor.h"

/* a backup version */
struct backupVersion {

	sds path;
	int32_t bv_num; /* backup version number start from 0 */

	int deleted;

	int64_t number_of_files;
	int64_t number_of_chunks;

	sds fname_prefix; /* The prefix of the file names */

	FILE *metadata_fp;
	FILE *recipe_fp;
	FILE *seed_fp;
};

/* Point to the meta of a recipe */
struct recipeMeta {
	int64_t chunknum;
	int64_t filesize;
	sds filename;
};

/*
 * Each recipe consists of segments.
 * Each prefetched segment is organized as a hash table for optimizing lookup.
 * It is the basic unit of logical locality.
 * */
struct segmentRecipe {
	segmentid id;
	/* Map fingerprints in the segment to their container IDs.*/
	GHashTable *kvpairs;
	GHashTable *features;
};

/*
 * If id == CHUNK_SEGMENT_START or CHUNK_SEGMENT_END,
 * it is a flag of segment boundary.
 * If id == CHUNK_SEGMENT_START,
 * size indicates the length of the segment in terms of # of chunks.
 */
struct chunkPointer {
	fingerprint fp;
	containerid id;
	int32_t size;
};

void init_recipe_store();
void close_recipe_store();

struct backupVersion* create_backup_version(const char *path);
int backup_version_exists(int number);
struct backupVersion* open_backup_version(int number);
void update_backup_version(struct backupVersion *b);
void free_backup_version(struct backupVersion *b);

void append_recipe_meta(struct backupVersion* b, struct recipeMeta* r);
void append_n_chunk_pointers(struct backupVersion* b,
		struct chunkPointer* cp, int n);
struct recipeMeta* read_next_recipe_meta(struct backupVersion* b);
struct chunkPointer* read_next_n_chunk_pointers(struct backupVersion* b, int n,
		int *k);
void append_seed(struct backupVersion* b, containerid id, int32_t size);
containerid* read_next_n_seeds(struct backupVersion* b, int n, int *k);
struct recipeMeta* new_recipe_meta(char* name);
void free_recipe_meta(struct recipeMeta* r);

int segment_recipe_check_id(struct segmentRecipe* sr, segmentid *id);
struct segmentRecipe* new_segment_recipe();
void free_segment_recipe(struct segmentRecipe* sr);
segmentid append_segment_flag(struct backupVersion* b, int flag, int segment_size);
GQueue* prefetch_segments(segmentid id, int prefetch_num);
int lookup_fingerprint_in_segment_recipe(struct segmentRecipe* sr,
        fingerprint *fp);

#endif /* RECIPESTORE_H_ */
