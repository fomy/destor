/*
 * lru_cache.h
 *	GList-based lru cache
 *  Created on: May 23, 2012
 *      Author: fumin
 */

#ifndef Cache_H_
#define Cache_H_
#define INFI_Cache -1

#include <glib.h>

struct lruCache {
	GList *elem_queue;

	int cache_max_size; // less then zero means infinite cache
	int cache_size;

	double hit_count;
	double miss_count;

	void (*free_elem)(void *);
	int (*hit_elem)(void* elem, void* user_data);
};

struct lruCache* new_lru_cache(int size, void (*free_elem)(void *),
		int (*hit_elem)(void* elem, void* user_data));
void free_lru_cache(struct lruCache*);
void* lru_cache_lookup(struct lruCache*, void* user_data);
void* lru_cache_hits(struct lruCache*, void* user_data,
		int (*hit)(void* elem, void* user_data));
void lru_cache_insert(struct lruCache *c, void* data,
		void (*victim)(void*, void*), void* user_data);

#endif /* Cache_H_ */
