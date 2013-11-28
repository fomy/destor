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

struct {
	/* lru queue of cached containers */
	struct lruCache *queue;

	int seed_count;

	/* buffered seeds */
	int win_size;
	GQueue *window;

	/*
	 * Sequence of access records.
	 * Being sorted in distance order.
	 * When read more seeds,
	 * sort it in containerid order for updating distance.
	 * */
	GSequence *distance_seq;
} optimal_cache;

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

static gint g_access_record_cmp_by_distance(struct accessRecord *a,
		struct accessRecord *b, gpointer data) {
	int *da = g_queue_peak_head(a->distance_queue);
	int *db = g_queue_peak_head(b->distance_queue);
	return *da - *db;
}

static containerid optimal_cache_window_slides() {
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

				GSequenceIter *iter = g_sequence_lookup(
						optimal_cache.distance_seq, &ids[i], g_containerid_cmp,
						NULL);
				struct accessRecord *r;
				if (!iter) {
					r = new_access_record(ids[i]);
					g_sequence_insert_sorted(optimal_cache.distance_seq,
							&r->cid, g_containerid_cmp, NULL);
				} else {
					containerid *id = g_sequence_get(iter);
					r = id - offsetof(struct accessRecord, cid);
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
	}

	GSequenceIter *iter = g_sequence_lookup(optimal_cache.distance_seq, &id,
			g_containerid_cmp, NULL);
	assert(iter);
	containerid *p = g_sequence_get(iter);

	struct accessRecord *r = p - offsetof(struct accessRecord, cid);
	g_queue_pop_head(r->distance_queue);
	if (g_queue_get_length(r->distance_queue) == 0) {
		free_access_record(r);
		g_sequence_remove(iter);
	}

	return id;
}

void init_optimal_cache() {
	if (destor.simulation_level == SIMULATION_NO)
		optimal_cache.queue = new_lru_cache(destor.restore_cache[1],
				free_container, lookup_fingerprint_in_container);
	else
		optimal_cache.queue = new_lru_cache(destor.restore_cache[1],
				free_container_meta, lookup_fingerprint_in_container_meta);

	optimal_cache.window = g_queue_new();
	optimal_cache.win_size = 0;
	optimal_cache.seed_count = 0;

	optimal_cache.distance_seq = g_sequence_new(NULL);
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
