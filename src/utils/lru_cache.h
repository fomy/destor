/*
 * lru_cache.h
 *	GList-based lru cache
 *  Created on: May 23, 2012
 *      Author: fumin
 */

#ifndef Cache_H_
#define Cache_H_
#define INFI_CACHE -1

#include <glib.h>

struct lruCache {
	GList *elem_queue;

	int max_size; // less then zero means infinite cache
	int size;

	double hit_count;
	double miss_count;

	void (*free_elem)(void *);
	int (*hit_elem)(void* elem, void* user_data);
};

struct lruCache* new_lru_cache(int size, void (*free_elem)(void *),
		int (*hit_elem)(void* elem, void* user_data));
void free_lru_cache(struct lruCache*);
void* lru_cache_lookup(struct lruCache*, void* user_data);
void* lru_cache_lookup_without_update(struct lruCache* c, void* user_data);
void* lru_cache_hits(struct lruCache* c, void* user_data,
		int (*hit)(void* elem, void* user_data));
/* Kick the elem that makes func returning 1. */
void lru_cache_kicks(struct lruCache* c, void* user_data,
		int (*func)(void* elem, void* user_data));
void lru_cache_insert(struct lruCache *c, void* data,
		void (*victim)(void*, void*), void* user_data);
int lru_cache_is_full(struct lruCache*);

#endif /* Cache_H_ */
