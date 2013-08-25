/**
 * @file cfl_monitor.c
 * @Synopsis  only used in cfl_monitor
 * @author fumin, fumin@hust.edu.cn
 * @version 1
 * @date 2012-12-12
 */
#include "cfl_monitor.h"
#include "container.h"

static BOOL container_id_equal(ContainerId* a, ContainerId* b) {
	return *a == *b ? TRUE : FALSE;
}

CFLMonitor* cfl_monitor_new(int read_cache_size) {
	CFLMonitor *monitor = (CFLMonitor*) malloc(sizeof(CFLMonitor));
	monitor->total_size = 0;

	monitor->ccf = 0;
	monitor->ocf = 0;
	monitor->cfl = 0;
	monitor->cache = lru_cache_new(read_cache_size);
	return monitor;
}

void cfl_monitor_free(CFLMonitor *monitor) {
	lru_cache_free(monitor->cache, free);
	monitor->cache = 0;
	free(monitor);
}

/*
 * Maintain a LRU cache internally to simulate recovery process when backing-up.
 */
void update_cfl(CFLMonitor *monitor, ContainerId id, int32_t chunklen) {
	monitor->total_size += chunklen + CONTAINER_META_ENTRY_SIZE;

	void *key = lru_cache_lookup(monitor->cache, container_id_equal, &id);
	if (!key) {
		ContainerId *new_id = (ContainerId*) malloc(sizeof(ContainerId));
		*new_id = id;
		ContainerId *oldid = lru_cache_insert(monitor->cache, new_id);
		if (oldid)
			free(oldid);
		monitor->ccf++;
		monitor->total_size += CONTAINER_DES_SIZE;
	}
	/*
	 * This formula used to calculate ocf is not perfect.
	 * Because of the blank hole at the end of container,
	 * ocf can not be measured exactly.
	 * However, as the number of containers increases,
	 * the results becomes more and more exact.
	 * */
	monitor->ocf = (monitor->total_size + CONTAINER_SIZE - 1) / CONTAINER_SIZE;
	monitor->cfl = monitor->ocf / (double) monitor->ccf;
}

/* 
 * No LRU cache.
 * Used in restore and optimal cache 
 * */
void update_cfl_directly(CFLMonitor* monitor, int32_t chunklen, BOOL is_new) {
	monitor->total_size += chunklen + CONTAINER_META_ENTRY_SIZE;
	if (is_new) {
		monitor->ccf++;
		monitor->total_size += CONTAINER_DES_SIZE;
	}

	monitor->ocf = (monitor->total_size + CONTAINER_SIZE - 1) / CONTAINER_SIZE;
	monitor->cfl = monitor->ocf / (double) monitor->ccf;
}

BOOL is_container_already_in_cache(CFLMonitor* monitor, ContainerId id) {
	void *key = lru_cache_lookup_without_update(monitor->cache,
			container_id_equal, &id);
	return key ? TRUE : FALSE;
}

double get_cfl(CFLMonitor *monitor) {
	return monitor->cfl > 1 ? 1 : monitor->cfl;
}

void print_cfl(CFLMonitor *monitor) {
	printf("cfl=%.3lf, ocf=%d, ccf=%d\n", get_cfl(monitor), monitor->ocf,
			monitor->ccf);
}
