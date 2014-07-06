/**
 * @file cfl_monitor.c
 * @Synopsis  only used in cfl_monitor
 * @author fumin, fumin@hust.edu.cn
 * @version 1
 * @date 2012-12-12
 */
#include "rewrite_phase.h"
#include "storage/containerstore.h"
#include "utils/lru_cache.h"

struct {
	int64_t total_size;

	int ocf; //data amount/CONTAINER_SIZE
	int ccf;
	double cfl; //ocf/ccf

	struct lruCache *cache;
} monitor;

static int container_record_check_id(struct containerRecord* a,
		containerid *id){
	return a->cid == *id ? 1 : 0;
}

/*static int container_record_equal(struct containerRecord* a,
		struct containerRecord* b) {
	return a->cid == b->cid ? 1 : 0;
}*/

void init_restore_aware() {
	monitor.total_size = 0;

	monitor.ccf = 0;
	monitor.ocf = 0;
	monitor.cfl = 0;
	monitor.cache = new_lru_cache(destor.restore_cache[1], free,
			container_record_check_id);
}

/*
 * Maintain a LRU cache internally to simulate recovery process when backing-up.
 */
void restore_aware_update(containerid id, int32_t chunklen) {
	monitor.total_size += chunklen + CONTAINER_META_ENTRY;

	struct containerRecord* record = lru_cache_lookup(monitor.cache, &id);
	if (!record) {
		record = (struct containerRecord*) malloc(
				sizeof(struct containerRecord));
		record->cid = id;
		lru_cache_insert(monitor.cache, record, NULL, NULL);

		monitor.ccf++;
	}

	monitor.ocf = (monitor.total_size + CONTAINER_SIZE - 1) / CONTAINER_SIZE;
	monitor.cfl = monitor.ocf / (double) monitor.ccf;
}

int restore_aware_contains(containerid id) {
	return lru_cache_lookup_without_update(monitor.cache, &id) ? 1 : 0;
}

double restore_aware_get_cfl() {
	return monitor.cfl > 1 ? 1 : monitor.cfl;
}
