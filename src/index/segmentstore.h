/*
 * segmentstore.h
 *
 *  Created on: Nov 24, 2013
 *      Author: fumin
 */

#ifndef SEGMENTSTORE_H_
#define SEGMENTSTORE_H_

#include "index.h"

struct indexElem {
	containerid id;
	fingerprint fp;
};

/*
 * Each prefetched segment is organized as a hash table for optimizing lookup.
 * It is the basic unit of segment store.
 * */
struct segmentRecipe {
	segmentid id;
	/* Map fingerprints in the segment to their container IDs.*/
	GHashTable *kvpairs;
	GHashTable *features;
	pthread_mutex_t mutex;
	int reference_count;
};

struct segmentRecipe* new_segment_recipe();
struct segmentRecipe* new_segment_recipe_full(struct segment* s);
struct segmentRecipe* ref_segment_recipe(struct segmentRecipe*);
void unref_segment_recipe(struct segmentRecipe*);
void free_segment_recipe(struct segmentRecipe*);
int lookup_fingerprint_in_segment_recipe(struct segmentRecipe*, fingerprint *);
int segment_recipe_check_id(struct segmentRecipe*, segmentid *id);
struct segmentRecipe* segment_recipe_dup(struct segmentRecipe*);
struct segmentRecipe* segment_recipe_merge(struct segmentRecipe*, struct segmentRecipe*);

void init_segment_management();

void close_segment_management();

struct segmentRecipe* retrieve_segment(segmentid);
GQueue* prefetch_segments(segmentid id, int prefetch_num);

struct segmentRecipe* update_segment(struct segmentRecipe*);

#endif /* SEGMENTSTORE_H_ */
