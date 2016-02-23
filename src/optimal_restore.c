/*
 * optimal_restore.c
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */
#include "destor.h"
#include "jcr.h"
#include "recipe/recipestore.h"
#include "storage/containerstore.h"
#include "restore.h"
#include "utils/lru_cache.h"

/* Consisting of a sequence of access records with an identical ID */
struct accessRecords {
	containerid cid;
	/* A queue of sequence numbers */
	GQueue *seqno_queue;
};

static struct accessRecords* new_access_records(containerid id) {
	struct accessRecords* r = (struct accessRecords*) malloc(
			sizeof(struct accessRecords));
	r->cid = id;
	r->seqno_queue = g_queue_new();
	return r;
}

static void free_access_records(struct accessRecords* r) {
	assert(g_queue_get_length(r->seqno_queue) == 0);
	g_queue_free_full(r->seqno_queue, free);
	free(r);
}

/*
 * Ascending order.
 */
static gint g_access_records_cmp_by_first_seqno(struct accessRecords *a,
		struct accessRecords *b, gpointer data) {
	int *da = g_queue_peek_head(a->seqno_queue);
	if (da == NULL)
		return 1;

	int *db = g_queue_peek_head(b->seqno_queue);
	if (db == NULL)
		return -1;

	return *da - *db;
}

struct {

	/* the seqno of next read access record */
	int current_sequence_number;

	/* Index buffered access records by id. */
	GHashTable *access_record_table;
	int buffered_access_record_num;

	/* Access records of cached containers. */
	GSequence *sorted_records_of_cached_containers;

	/* Container queue. */
	struct lruCache *lru_queue;

} optimal_cache;

static void optimal_cache_window_fill() {
	int n = destor.restore_opt_window_size - optimal_cache.buffered_access_record_num, k;
	containerid *ids = read_next_n_records(jcr.bv, n, &k);
	if (ids) {
		optimal_cache.buffered_access_record_num += k;

		/* update distance_seq */
		int i;
		for (i = 1; i <= k; i++) {

			struct accessRecords* r = g_hash_table_lookup(
					optimal_cache.access_record_table, &ids[i]);
			if (!r) {
				r = new_access_records(ids[i]);
				g_hash_table_insert(optimal_cache.access_record_table, &r->cid, r);
			}

			int *no = (int*) malloc(sizeof(int));
			*no = optimal_cache.current_sequence_number++;
			g_queue_push_tail(r->seqno_queue, no);

		}
	}
}

/*
 * Init the optimal cache.
 */
void init_optimal_cache() {
	optimal_cache.current_sequence_number = 0;

	optimal_cache.access_record_table = g_hash_table_new_full(g_int64_hash, g_int64_equal,
			NULL, free_access_records);
	optimal_cache.buffered_access_record_num = 0;

	optimal_cache.sorted_records_of_cached_containers = g_sequence_new(NULL);

	if (destor.simulation_level == SIMULATION_NO)
		optimal_cache.lru_queue = new_lru_cache(destor.restore_cache[1],
				free_container, lookup_fingerprint_in_container);
	else
		optimal_cache.lru_queue = new_lru_cache(destor.restore_cache[1],
				free_container_meta, lookup_fingerprint_in_container_meta);

	optimal_cache_window_fill();
}

static void optimal_cache_window_slides(containerid id) {

	if (optimal_cache.buffered_access_record_num  * 2 <= destor.restore_opt_window_size)
		optimal_cache_window_fill();

	struct accessRecords *r = g_hash_table_lookup(optimal_cache.access_record_table, &id);
	assert(r);
	int *d = g_queue_pop_head(r->seqno_queue);
	free(d);
	optimal_cache.buffered_access_record_num--;

}

/*
 * Before looking up a fingerprint,
 * we call optimal_cache_hits to check whether
 * the target container is in the cache.
 */
static int optimal_cache_hits(containerid id) {
	static containerid last_id = TEMPORARY_ID;
	if (last_id != id) {
		optimal_cache_window_slides(id);
		last_id = id;
	}

	if (destor.simulation_level == SIMULATION_NO)
		return lru_cache_hits(optimal_cache.lru_queue, &id,
				container_check_id) == NULL ? 0 : 1;
	else
		return lru_cache_hits(optimal_cache.lru_queue, &id,
				container_meta_check_id) == NULL ? 0 : 1;
}

/* The function will not be called if the simulation level >= RESTORE. */
static struct chunk* optimal_cache_lookup(fingerprint *fp) {

	assert(destor.simulation_level == SIMULATION_NO);

	struct container* con = lru_cache_lookup(optimal_cache.lru_queue, fp);
	struct chunk* c = get_chunk_in_container(con, fp);
	assert(c);

	return c;
}

struct accessRecords* victim;

static int find_kicked_container(struct container* con, GHashTable *ht) {

	struct accessRecords* r = g_hash_table_lookup(ht, &con->meta.id);
	if(r){
		victim = r;
		return 1;
	}
	return 0;
}

static int find_kicked_container_meta(struct containerMeta* cm, GHashTable *ht) {

	struct accessRecords* r = g_hash_table_lookup(ht, &cm->id);
	if(r){
		victim = r;
		return 1;
	}
	return 0;
}

static void optimal_cache_insert(containerid id) {

	if (lru_cache_is_full(optimal_cache.lru_queue)) {
		GHashTable* ht = g_hash_table_new(g_int64_hash, g_int64_equal);

		/*
		 * re-sort the sequence.
		 */
		g_sequence_sort(optimal_cache.sorted_records_of_cached_containers,
				g_access_records_cmp_by_first_seqno, NULL);

		GSequenceIter *iter = g_sequence_iter_prev(
				g_sequence_get_end_iter(optimal_cache.sorted_records_of_cached_containers));
		struct accessRecords* r = g_sequence_get(iter);
		g_hash_table_insert(ht, &r->cid, r);

		int i = 0;
		while (i < 10 && iter != g_sequence_get_begin_iter(optimal_cache.sorted_records_of_cached_containers)) {
			iter = g_sequence_iter_prev(iter);
			r = g_sequence_get(iter);
			if (g_queue_get_length(r->seqno_queue) == 0)
				g_hash_table_insert(ht, &r->cid, r);
			else
				break;
			i++;
		}

		if (destor.simulation_level == SIMULATION_NO)
			lru_cache_kicks(optimal_cache.lru_queue, ht, find_kicked_container);
		else
			lru_cache_kicks(optimal_cache.lru_queue, ht,
					find_kicked_container_meta);

		g_hash_table_destroy(ht);

		iter = g_sequence_iter_prev(
				g_sequence_get_end_iter(optimal_cache.sorted_records_of_cached_containers));
		r = g_sequence_get(iter);
		while (r->cid != victim->cid) {
			iter = g_sequence_iter_prev(iter);
			r = g_sequence_get(iter);
		}

		/* iter (r) points to the evicted record */
		if(g_queue_get_length(r->seqno_queue) == 0){
			/* the container will not be accessed in the future */
			g_hash_table_remove(optimal_cache.access_record_table, &r->cid);
		}

		g_sequence_remove(iter);
	}

	jcr.read_container_num++;
	if (destor.simulation_level == SIMULATION_NO) {
		struct container* con = retrieve_container_by_id(id);
		lru_cache_insert(optimal_cache.lru_queue, con, NULL, NULL);
	} else {
		struct containerMeta *cm = retrieve_container_meta_by_id(id);
		lru_cache_insert(optimal_cache.lru_queue, cm, NULL, NULL);
	}

	struct accessRecords* r = g_hash_table_lookup(optimal_cache.access_record_table, &id);
	assert(r);

	g_sequence_insert_sorted(optimal_cache.sorted_records_of_cached_containers, r,
			g_access_records_cmp_by_first_seqno, NULL);

}

void* optimal_restore_thread(void *arg) {
	init_optimal_cache();

	struct chunk* c;
	while ((c = sync_queue_pop(restore_recipe_queue))) {

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			sync_queue_push(restore_chunk_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (!optimal_cache_hits(c->id)) {
			VERBOSE("Restore cache: container %lld is missed", c->id);
			optimal_cache_insert(c->id);
		}

		if (destor.simulation_level == SIMULATION_NO) {
			struct chunk* rc = optimal_cache_lookup(&c->fp);
			TIMER_END(1, jcr.read_chunk_time);
			sync_queue_push(restore_chunk_queue, rc);
		} else {
			TIMER_END(1, jcr.read_chunk_time);
		}

		jcr.data_size += c->size;
		jcr.chunk_num++;
		free_chunk(c);
	}

	sync_queue_term(restore_chunk_queue);
	return NULL;
}
