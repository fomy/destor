/*
 * Hash Table for index module
 * It is designed for Fingerprint-ContainerId store.
 *
 */

#ifndef __HTABLE__
#define __HTABLE__
#include "../global.h"
#include "../dedup.h"

typedef struct hlink {
	Fingerprint key;
	ContainerId value;
	struct hlink *next;
} hlink;

typedef struct htable {
	hlink **table; /* hash table */
	int64_t num_items; /* current number of items */
	int64_t max_items; /* maximum items before growing */
	int64_t buckets; /* size of hash table */
	hlink *walkptr; /* table walk pointer */
	int64_t walk_index; /* table walk index */
} HTable;

HTable *htable_new(int64_t tsize);
void htable_destroy(HTable*);
void htable_insert(HTable*, Fingerprint*, ContainerId);
ContainerId* htable_lookup(HTable*, Fingerprint*);
hlink *htable_first(HTable*); /* get first item in table */
hlink *htable_next(HTable *); /* get next item in table */
void htable_stats(HTable*); /* print stats about the table */
int64_t htable_size(HTable*); /* return size of table */

#endif
