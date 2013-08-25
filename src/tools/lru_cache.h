/*
 * lru_cache.h
 *	GList-based lru cache
 *  Created on: May 23, 2012
 *      Author: fumin
 */

#ifndef Cache_H_
#define Cache_H_
#define INFI_Cache -1

#include "../global.h"
#include <glib.h>

typedef struct lru_cache_tag LRUCache;

struct lru_cache_tag {
	GList *lru_queue;

	int cache_max_size; // less then zero means infinite cache
	int cache_size;

	double hit_count;
	double miss_count;

	void (*data_free)(void *);
};

LRUCache* lru_cache_new(int size);
void lru_cache_free(LRUCache *cache, void (*data_free)(void*));
void* lru_cache_lookup(LRUCache *cache,
		BOOL (*condition_func)(void* item, void* user_data), void* user_data);
void* lru_cache_lookup_without_update(LRUCache *cache,
		BOOL (*condition_func)(void* item, void* user_data), void* user_data);
void* lru_cache_insert(LRUCache *cache, void* data);
void lru_cache_foreach(LRUCache *cache, GFunc func, gpointer user_data);
#endif /* Cache_H_ */
