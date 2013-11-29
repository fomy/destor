/*
 * optimal_restore.c
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */
#include "destor.h"
#include "jcr.h"
#include "recipe/recipemanage.h"
#include "storage/container_manage.h"
#include "restore.h"

struct accessRecord {
	containerid cid;
	GQueue *distance_queue;
};

static struct accessRecord* new_access_record(containerid id) {
	struct accessRecord* r = (struct accessRecord*) malloc(
			sizeof(struct accessRecord));
	r->cid = id;
	r->distance_queue = g_queue_new();
	return r;
}

static void free_access_record(struct accessRecord* r) {
	assert(g_queue_get_length(r->distance_queue) == 0);
	g_queue_free_full(r->distance_queue, free);
	free(r);
}

static gint g_access_record_cmp_by_id(struct accessRecord *a,
		struct accessRecord *b, gpointer data) {
	return a->cid - b->cid;
}

static gint g_containerid_cmp(containerid *a, containerid *b, gpointer data) {
	return *a - *b;
}

/*
 * Ascending order.
 */
static gint g_access_record_cmp_by_distance(struct accessRecord *a,
		struct accessRecord *b, gpointer data) {
	int *da = g_queue_peak_head(a->distance_queue);
	if (da == NULL)
		return 1;

	int *db = g_queue_peak_head(b->distance_queue);
	if (db == NULL)
		return -1;

	return *da - *db;
}

struct {

	/* the distance of next access record */
	int seed_count;

	/* buffered seeds */
	int win_size;
	GQueue *window;

	/* Index bufferred seeds by id. */
	GHashTable *seed_table;

	/* Access records of cached containers. */
	GSequence *distance_seq;

	/* Container queue. */
	struct lruCache lru_queue;

} optimal_cache;

void init_optimal_cache() {
	optimal_cache.seed_count = 0;

	optimal_cache.window = g_queue_new();
	optimal_cache.win_size = 0;

	optimal_cache.seed_table = g_hash_table_new(g_int64_hash, g_int64_equal,
	NULL, free_access_record);

	optimal_cache.distance_seq = g_sequence_new(NULL);

	if (destor.simulation_level == SIMULATION_NO)
		optimal_cache.lru_queue = new_lru_cache(destor.restore_cache[1],
				free_container, lookup_fingerprint_in_container);
	else
		optimal_cache.lru_queue = new_lru_cache(destor.restore_cache[1],
				free_container_meta, lookup_fingerprint_in_container_meta);
}

static containerid optimal_cache_window_slides() {
	/* The position of the current seed */
	static int cur;

	if (optimal_cache.win_size * 2 <= destor.restore_opt_window_size) {
		int n = destor.restore_opt_window_size - optimal_cache.win_size, k;
		containerid *ids = read_next_n_seeds(jcr.bv, n, &k);
		if (ids) {
			g_queue_push_tail(optimal_cache.window, ids);
			optimal_cache.win_size += k;

			/* update distance_seq */
			int i;
			for (i = 0; i < k; i++) {

				struct accessRecord* r = g_hash_table_lookup(
						optimal_cache.seed_table, &ids[i]);
				if (!r) {
					r = new_access_record(ids[i]);
					g_hash_table_insert(optimal_cache.seed_table, &r->cid, r);
				}

				int *d = (int*) malloc(sizeof(int));
				*d = optimal_cache.seed_count++;
				g_queue_push_tail(r->distance_queue, d);

			}
		}
	}

	containerid *ids = g_queue_peak_head(optimal_cache.window);
	containerid id = ids[cur++];
	optimal_cache.win_size--;
	if (cur == g_queue_get_length(ids)) {
		g_queue_pop_head(optimal_cache.window);
		free(ids);
		cur = 0;
	}

	struct accessRecord *r = g_hash_table_lookup(optimal_cache.seed_table, &id);
	assert(r);
	int *d = g_queue_pop_head(r->distance_queue);
	free(d);

	/*
	 * Because many accessRecords are updated,
	 * we sort the sequence.
	 */
	g_sequence_sort(optimal_cache.distance_seq, g_access_record_cmp_by_distance,
	NULL);

	return id;
}

/*
 * Before looking up a fingerprint,
 * we call optimal_cache_hits to check whether
 * the target container is in the cache.
 */
static int optimal_cache_hits(containerid id) {
	return lru_cache_hits(optimal_cache.lru_queue, container_check_id, &id);
}

/* The function will not be called if the simulation level >= RESTORE. */
static struct chunk* optimal_cache_lookup(fingerprint *fp) {

	assert(destor.simulation_level == SIMULATION_NO);

	struct container* con = lru_cache_lookup(optimal_cache.lru_queue, fp);
	struct chunk* c = get_chunk_in_container(con, fp);
	assert(c);

	return c;
}

static int find_kicked_container(struct container* con, GQueue *q) {
	int i, n = g_queue_get_length(q);
	for (i = 0; i < n; i++) {
		struct accessRecord* r = g_queue_peak_nth(i);
		if (container_check_id(con, &r->cid))
			return 1;
	}
	return 0;
}

static int find_kicked_container_meta(struct containerMeta* cm, GQueue *q) {
	int i, n = g_queue_get_length(q);
	for (i = 0; i < n; i++) {
		struct accessRecord* r = g_queue_peak_nth(i);
		if (container_meta_check_id(cm, &r->cid)) {
			g_queue_clear(q);
			g_queue_push_tail(q, r);
			return 1;
		}
	}
	return 0;
}

static void optimal_cache_insert(struct container* con) {

	if (lru_cache_is_full(optimal_cache.lru_queue)) {
		GQueue *q = g_queue_new();

		GSequenceIter *iter = g_sequence_iter_prev(
				g_sequence_get_end_iter(optimal_cache.distance_seq));
		struct accessRecord* r = g_sequence_get(iter);
		do {
			g_queue_push_tail(r);
			iter = g_sequence_iter_prev(iter);
			r = g_sequence_get(iter);
		} while (g_queue_get_length(r->distance_queue) == 0);

		if (destor.simulation_level == SIMULATION_NO)
			lru_cache_kicks(optimal_cache.lru_queue, find_kicked_container, q);
		else
			lru_cache_kicks(optimal_cache.lru_queue, find_kicked_container_meta,
					q);

		struct accessRecord* victim = g_queue_pop_head(q);
		q_queue_free(q);

		iter = g_sequence_iter_prev(
				g_sequence_get_end_iter(optimal_cache.distance_seq));
		r = g_sequence_get(iter);
		while (r->cid != victim->cid) {
			iter = g_sequence_iter_prev(iter);
		}

		g_sequence_remove(iter);
	}

	lru_cache_insert(optimal_cache.lru_queue, con, NULL, NULL);
	struct accessRecord* r = g_hash_table_lookup(optimal_cache.seed_table,
			&con->meta.id);
	assert(r);

	g_sequence_insert_sorted(optimal_cache.distance_seq, r,
			g_access_record_cmp_by_distance, NULL);

}

void* optimal_restore_thread(void *arg) {
	init_optimal_cache();

	struct recipe* r = NULL;
	struct chunkPointer* cp = NULL;

	int sig = recv_restore_recipe(&r);

	while (sig != STREAM_END) {

		struct chunk *c = new_chunk(sdslen(r->filename) + 1);
		strcpy(c->data, r->filename);
		c->size = -(sdslen(r->filename) + 1);
		send_restore_chunk(c);

		int num = r->chunknum, i;
		for (i = 0; i < num; i++) {
			sig = recv_restore_recipe(&cp);

			if (destor.simulation_level >= SIMULATION_RESTORE) {

			} else {

			}
		}

		free_recipe(r);
		sig = recv_restore_recipe(&r);
	}

	term_restore_chunk_queue();
	return NULL;
}
