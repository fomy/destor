#include "htable.h"
/*
 * Take each hash link and walk down the chain of items
 *  that hash there counting them (i.e. the hits), 
 *  then report that number.
 *  Obiously, the more hits in a chain, the more time
 *  it takes to reference them. Empty chains are not so
 *  hot either -- as it means unused or wasted space.
 */
#define MAX_COUNT 20

/*
 * key's offset in item, length of key, table size
 */
HTable* htable_new(int64_t tsize) {
	HTable *htable = (HTable*) malloc(sizeof(HTable));

	int pwr;
	tsize >>= 2;
	for (pwr = 0; tsize; pwr++) {
		tsize >>= 1;
	}

	htable->buckets = 1 << pwr; /* hash table size -- power of two */

	htable->num_items = 0; /* number of entries in table */
	htable->max_items = htable->buckets * 4; /* allow average 4 entries per chain */
	htable->table = (hlink **) malloc(htable->buckets * sizeof(hlink *));
	memset(htable->table, 0, htable->buckets * sizeof(hlink *));
	htable->walkptr = NULL;
	htable->walk_index = 0;

	return htable;
}

int64_t htable_size(HTable* htable) {
	return htable->num_items;
}

void htable_stats(HTable *htable) {
	int hits[MAX_COUNT];
	int max = 0;
	int64_t i, j;
	hlink *p;
	printf("\n\nNumItems=%lld\nTotal buckets=%lld\n", htable->num_items,
			htable->buckets);
	printf("Hits/bucket: buckets\n");
	for (i = 0; i < MAX_COUNT; i++) {
		hits[i] = 0;
	}
	for (i = 0; i < htable->buckets; i++) {
		p = htable->table[i];
		j = 0;
		while (p) {
			p = p->next;
			j++;
		}
		if (j > max) {
			max = j;
		}
		if (j < MAX_COUNT) {
			hits[j]++;
		}
	}
	for (i = 0; i < MAX_COUNT; i++) {
		printf("%lld:           %d\n", i, hits[i]);
	}
	printf("max hits in a bucket = %d\n", max);
}

static void htable_grow(HTable *htable) {
	/* Setup a bigger table */
	HTable *big = (HTable *) malloc(sizeof(HTable));
	big->num_items = 0;
	big->buckets = htable->buckets * 2;
	big->max_items = big->buckets * 4;
	big->table = (hlink **) malloc(big->buckets * sizeof(hlink *));
	memset(big->table, 0, big->buckets * sizeof(hlink *));
	big->walkptr = NULL;
	big->walk_index = 0;

	/* Insert all the items in the new hash table */
	/*
	 * We walk through the old smaller tree getting items,
	 * but since we are overwriting the colision links, we must
	 * explicitly save the item->next pointer and walk each
	 * colision chain ourselves.  We do use next() for getting
	 * to the next bucket.
	 */
	hlink *item = htable_first(htable);
	while (item) {
		htable_insert(big, &item->key, item->value);
		item = htable_next(htable);
	}

	free(htable->table);
	memcpy(htable, big, sizeof(HTable)); /* move everything across */
	free(big);
}

void htable_insert(HTable *htable, Fingerprint *key, ContainerId value) {
	if (key == NULL) {
		dprint("key is NULL!");
		return;
	}

	hlink *hp = (hlink*) malloc(sizeof(hlink));
	memcpy(&hp->key, key, sizeof(Fingerprint));
	memcpy(&hp->value, &value, sizeof(ContainerId));
	hp->next = NULL;

	int64_t index = *(int64_t*) key;
	index = index % htable->buckets;
	if (index < 0)
		index += htable->buckets;

	hp->next = htable->table[index]; /*some pro*/
	htable->table[index] = hp;

	if (++htable->num_items >= htable->max_items) {
		htable_grow(htable);
	}
}

ContainerId* htable_lookup(HTable *htable, Fingerprint *key) {
	int64_t index = *(int64_t*) key;
	index = index % htable->buckets;
	if (index < 0)
		index += htable->buckets;

	hlink *hp = htable->table[index];
	while (hp) {
		if (memcmp(key, &hp->key, sizeof(Fingerprint)) == 0) {
			return &hp->value;
		}
		hp = hp->next;
	}

	return NULL;
}

/*void *htable::remove(unsigned char *key)
 {
 hash_index(key);
 hlink *pre = table[index];
 for (hlink *hp=table[index]; hp; hp=(hlink *)hp->next)
 {
 if (memcmp(key, hp->key,hashlength) == 0) //hash == hp->hash &&
 {
 //Dmsg1(100, "lookup return %x\n", ((char *)hp)-loffset);
 if(pre==hp) {
 table[index] = (hlink*)hp->next;
 } else {
 pre->next = hp->next;
 }
 --num_items;
 return ((char *)hp)-loffset;
 }
 pre = hp;
 }
 return NULL;
 }*/

hlink* htable_next(HTable *htable) {
	if (htable->walkptr) {
		htable->walkptr = htable->walkptr->next;
	}
	while (!htable->walkptr && htable->walk_index < htable->buckets) {
		htable->walkptr = htable->table[htable->walk_index++];
	}
	if (htable->walkptr) {
		return htable->walkptr;
	}
	return NULL;
}

hlink* htable_first(HTable *htable) {
	htable->walkptr = htable->table[0]; /* get first bucket */
	htable->walk_index = 1; /* Point to next index */
	while (!htable->walkptr && htable->walk_index < htable->buckets) {
		htable->walkptr = htable->table[htable->walk_index++]; /* go to next bucket */
	}
	if (htable->walkptr) {
		return htable->walkptr;
	}
	return NULL;
}

/* Destroy the table and its contents */
void htable_destroy(HTable *htable) {
	hlink *ni;
	hlink *li = htable_first(htable);
	while (li) {
		ni = htable_next(htable);
		free(li);
		li = ni;
	}

	free(htable->table);
	free(htable);
}

