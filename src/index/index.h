/*
 * An index_update() to a fingerprint will be called after the index_search() to the fingerprint.
 * However, due to the existence of rewriting algorithms,
 * there is no guarantee that the index_update() will be called immediately after the index_search().
 * Thus, we would better make them independent with each other.
 *
 * The input of index_search is nearly same as that of index_update() except the ContainerId field.
 */
#ifndef INDEX_H_
#define INDEX_H_

#include "../destor.h"

struct indexElem {
	containerid id;
	fingerprint fp;
};

/* The buffer size > 2 * destor.rewrite_buffer_size */
/* All fingerprint that have been looked up in the index
 * but not been updated. */
struct {
	/* Queue of buffered segments and their features. */
	GQueue *segment_queue;
	/* map a fingerprint to a queue of indexElem */
	/* Index all fingerprints in the segment_queue. */
	GHashTable *table;
	/* The number of buffered chunks */
	int num;

	/* Buffer selected features in the current open container/segment. */
	GHashTable* buffered_features;
	/* If cid == TEMPORARY_ID, features are for segment. */
	containerid cid;
} index_buffer;

/*
 * The function is used to initialize memory structures of a fingerprint index.
 */
void init_index();
/*
 * Free memory structures and flush them into disks.
 */
void close_index();
/*
 * lookup fingerprints in a segment in index.
 */
void index_lookup(struct segment*);
/*
 * Insert fingerprint into Index for new fingerprint or new ContainerId.
 */
int index_update(fingerprint *fp, containerid from, containerid to);

void index_delete(fingerprint *);

GHashTable* (*featuring)(fingerprint *fp, int success);

/* Each prefetched segment is organized as a hash table for optimizing lookup. */
struct segmentRecipe {
	segmentid id;
	GHashTable* features;
	GHashTable *table;
};

struct segmentRecipe* new_segment_recipe();
void free_segment_recipe(struct segmentRecipe*);
int lookup_fingerprint_in_segment_recipe(struct segmentRecipe*, fingerprint *);
int segment_recipe_check_id(struct segmentRecipe*, segmentid *id);
struct segmentRecipe* segment_recipe_dup(struct segmentRecipe*);

#endif
