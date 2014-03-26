/*
 * fingerprint_cache.c
 *
 *  Created on: Mar 24, 2014
 *      Author: fumin
 */
#include "../destor.h"
#include "index.h"
#include "../storage/containerstore.h"
#include "segmentstore.h"
#include "../tools/lru_cache.h"

static struct lruCache* lru_queue;

int number_of_read_prefetch_unit;

void init_fingerprint_cache(){
	switch(destor.index_category[1]){
	case INDEX_CATEGORY_PHYSICAL_LOCALITY:
		lru_queue = new_lru_cache(destor.index_cache_size,
				free_container_meta, lookup_fingerprint_in_container_meta);
		break;
	case INDEX_CATEGORY_LOGICAL_LOCALITY:
		lru_queue = new_lru_cache(destor.index_cache_size,
				free_segment_recipe, lookup_fingerprint_in_segment_recipe);
		break;
	default:
		WARNING("Invalid index category!");
		exit(1);
	}

	number_of_read_prefetch_unit = 0;
}

int64_t fingerprint_cache_lookup(fingerprint *fp){
	switch(destor.index_category[1]){
		case INDEX_CATEGORY_PHYSICAL_LOCALITY:{
			struct containerMeta* cm = lru_cache_lookup(lru_queue, fp);
			if (cm)
				return cm->id;
			break;
		}
		case INDEX_CATEGORY_LOGICAL_LOCALITY:{
			struct segmentRecipe* sr = lru_cache_lookup(lru_queue, fp);
			if(sr){
				struct indexElem* e = g_hash_table_lookup(sr->kvpairs, fp);
				return e->id;
			}
			break;
		}
	}

	return TEMPORARY_ID;
}

void fingerprint_cache_prefetch(int64_t id){
	switch(destor.index_category[1]){
		case INDEX_CATEGORY_PHYSICAL_LOCALITY:{
			struct containerMeta * cm = retrieve_container_meta_by_id(id);
			if (cm) {
				number_of_read_prefetch_unit++;
				lru_cache_insert(lru_queue, cm, NULL, NULL);
			} else{
				WARNING("Error! The container %lld has not been written!", id);
				exit(1);
			}
			break;
		}
		case INDEX_CATEGORY_LOGICAL_LOCALITY:{
			number_of_read_prefetch_unit++;
			GQueue* segments = prefetch_segments(id,
					destor.index_segment_prefech);
			VERBOSE("Dedup phase: prefetch %d segments into %d cache",
					g_queue_get_length(segments),
					destor.index_cache_size);
			struct segmentRecipe* sr;
			while ((sr = g_queue_pop_tail(segments))) {
				/* From tail to head */
				if (!lru_cache_hits(lru_queue, &sr->id,
						segment_recipe_check_id)) {
					lru_cache_insert(lru_queue, sr, NULL, NULL);
				} else {
					 /* Already in cache */
					free_segment_recipe(sr);
				}
			}
			g_queue_free(segments);
			break;
		}
	}
}
