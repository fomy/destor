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
LRUCache* lru_cache_new(int size,
		gint (*data_cmp)(gconstpointer a, gconstpointer b)) {
	LRUCache* cache = (LRUCache*) malloc(sizeof(LRUCache));

	cache->lru_queue = NULL;

	cache->cache_max_size = size;
	cache->cache_size = 0;
	cache->hit_count = 0;
	cache->miss_count = 0;

	cache->data_cmp = data_cmp;

	return cache;
}

void lru_cache_free(LRUCache *cache, void (*data_free)(void *)) {
	/*printf("hit ratio:%.2f\n", ((double)cache->hit_count)/(cache->hit_count+cache->miss_count));*/
	g_list_free_full(cache->lru_queue, data_free);
	free(cache);
}

void* lru_cache_lookup(LRUCache *cache, void* data) {
	GList* item = g_list_find_custom(cache->lru_queue, data, cache->data_cmp);
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
void* lru_cache_lookup_without_update(LRUCache *cache, void* data) {
	GList* item = g_list_find_custom(cache->lru_queue, data, cache->data_cmp);
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
	/* Valgrind report it's possible to lost some memory here */
	cache->lru_queue = g_list_prepend(cache->lru_queue, data);
	cache->cache_size++;
	return evictor;
}

void lru_cache_foreach(LRUCache *cache, GFunc func, gpointer user_data) {
	g_list_foreach(cache->lru_queue, func, user_data);
}

void* lru_cache_first(LRUCache* cache) {
	cache->current_elem = g_list_first(cache->lru_queue);
	if (cache->current_elem)
		return cache->current_elem->data;
	else
		return NULL;
}
void* lru_cache_next(LRUCache* cache) {
	if (cache->current_elem) {
		cache->current_elem = g_list_next(cache->current_elem);
	}
	if (cache->current_elem)
		return cache->current_elem->data;
	else
		return NULL;
}
