/*
 * cache.c
 *
 *  Created on: May 23, 2012
 *      Author: fumin
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "lru_cache.h"

/*
 * The container read cache.
 */
LRUCache* lru_cache_new(int size) {
	LRUCache* cache = (LRUCache*) malloc(sizeof(LRUCache));

	cache->lru_queue = NULL;

	cache->cache_max_size = size;
	cache->cache_size = 0;
	cache->hit_count = 0;
	cache->miss_count = 0;

	return cache;
}

void lru_cache_free(LRUCache *cache, void (*data_free)(void *)) {
	g_list_free_full(cache->lru_queue, data_free);
	free(cache);
}

/* find a item in cache matching the condition */
void* lru_cache_lookup(LRUCache *cache,
		BOOL (*condition_func)(void* item, void* user_data), void* user_data) {
	GList* item = g_list_first(cache->lru_queue);
	while (item) {
		if (condition_func(item->data, user_data) != FALSE)
			break;
		item = g_list_next(item);
	}
	if (item) {
		cache->lru_queue = g_list_remove_link(cache->lru_queue, item);
		cache->lru_queue = g_list_concat(item, cache->lru_queue);
		cache->hit_count++;
		return item->data;
	} else {
		cache->miss_count++;
		return NULL;
	}
}

/*
 * Lookup data in lru cache,
 * but the lookup will not update the lru queue. 
 */
void* lru_cache_lookup_without_update(LRUCache *cache,
		BOOL (*condition_func)(void* item, void* user_data), void* user_data) {
	GList* item = g_list_first(cache->lru_queue);
	while (item) {
		if (condition_func(item->data, user_data) != FALSE)
			break;
		item = g_list_next(item);
	}
	if (item) {
		return item->data;
	} else {
		return NULL;
	}
}

/*
 * We know that the elem does not exist!
 */
void* lru_cache_insert(LRUCache *cache, void* data) {
	void *evictor = 0;
	if (cache->cache_max_size > 0
			&& cache->cache_size == cache->cache_max_size) {
		GList *last = g_list_last(cache->lru_queue);
		cache->lru_queue = g_list_remove_link(cache->lru_queue, last);
		evictor = last->data;
		g_list_free_1(last);
		cache->cache_size--;
	}
	/* Valgrind reports it's possible to lost some memory here */
	cache->lru_queue = g_list_prepend(cache->lru_queue, data);
	cache->cache_size++;
	return evictor;
}

BOOL lru_cache_contains(LRUCache *cache,
		BOOL (*equal)(void* elem, void* user_data), void* user_data) {
	GList* item = g_list_first(cache->lru_queue);
	while (item) {
		if (equal(item->data, user_data) == TRUE) {
			return TRUE;
		}
		item = g_list_next(item);
	}
	return FALSE;
}

void lru_cache_foreach(LRUCache *cache, GFunc func, gpointer user_data) {
	g_list_foreach(cache->lru_queue, func, user_data);
}

/*
 * iterate the cache until matching condition.
 */
void lru_cache_foreach_conditionally(LRUCache *cache,
		BOOL (*cond_func)(void* elem, void* user_data), void* user_data) {
	GList* item = g_list_first(cache->lru_queue);
	while (item) {
		if (cond_func(item->data, user_data) == TRUE) {
			cache->lru_queue = g_list_remove_link(cache->lru_queue, item);
			cache->lru_queue = g_list_concat(item, cache->lru_queue);
			break;
		}
		item = g_list_next(item);
	}
}

void* lru_cache_get_top(LRUCache *cache) {
	GList* item = g_list_first(cache->lru_queue);
	if (item)
		return item->data;
	return NULL;
}
