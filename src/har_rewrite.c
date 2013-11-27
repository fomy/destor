/*
 * har_rewrite.c
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */

#include "destor.h"
#include "rewrite_phase.h"
#include "storage/container_manage.h"
#include "jcr.h"

struct {
	/* Containers of enough utilization are in this map. */
	GHashTable *dense_map;
	/* Containers of not enough utilization are in this map. */
	GHashTable *sparse_map;

	int64_t total_size;
} container_utilization_monitor;

static GHashTable *inherited_sparse_containers;

void init_har() {

	container_utilization_monitor.dense_map = g_hash_table_new_full(
			g_int64_hash, g_int64_equal, NULL, free);
	container_utilization_monitor.sparse_map = g_hash_table_new_full(
			g_int64_hash, g_int64_equal, NULL, free);

	container_utilization_monitor.total_size = 0;

	inherited_sparse_containers = g_hash_table_new_full(g_int64_hash,
			g_int64_equal, NULL, free);

	/* The first backup doesn't have inherited sparse containers. */
	if (jcr.id == 0)
		return;

	sds fname = sdsdup(destor.working_directory);
	fname = sdscat(fname, "recipe/sparse");
	fname = sdscat(fname, itoa(jcr->id - 1));

	FILE* sparse_file = fopen(fname, "r+");

	if (sparse_file) {
		struct containerRecord tmp;
		while (fscanf(sparse_file, "%ld %d", &tmp.cid, &tmp.size) != EOF) {
			struct containerRecord *record = (struct containerRecord*) malloc(
					sizeof(struct containerRecord));
			memcpy(record, &tmp, sizeof(struct containerRecord));
			g_hash_table_insert(inherited_sparse_containers, &record->cid,
					record);
		}
		fclose(sparse_file);
	}

	sdsfree(fname);
}

void har_monitor_update(containerid id, int32_t size) {
	struct containerRecord* record = g_hash_table_lookup(
			container_utilization_monitor.dense_map, &id);
	if (record) {
		record->size += size;
	} else {
		record = g_hash_table_lookup(container_utilization_monitor.sparse_map,
				&id);
		if (!record) {
			record = (struct containerRecord*) malloc(
					sizeof(struct containerRecord));
			record->cid = id;
			record->size = 0;
			g_hash_table_insert(container_utilization_monitor.sparse_map,
					&record->cid, record);
		}
		record->size += size;
		double usage = record->size / (double) CONTAINER_SIZE;
		if (usage > destor.rewrite_har_utilization_threshold) {
			g_hash_table_steal(container_utilization_monitor.sparse_map,
					&record->cid);
			g_hash_table_insert(container_utilization_monitor.dense_map,
					&record->cid, record);
		}
	}
}

void close_har() {
	sds fname = sdsdup(destor.working_directory);
	fname = sdscat(fname, "recipe/sparse");
	fname = sdscat(fname, itoa(jcr->id));

	GHashTableIter iter;
	containerid* key;
	struct containerRecord* value;
	FILE* fp = fopen(fname, "w+");

	/* sparse containers */
	int inherited_sparse_num = 0;
	g_hash_table_iter_init(&iter, container_utilization_monitor.sparse_map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		if (inherited_sparse_containers
				&& g_hash_table_lookup(inherited_sparse_containers,
						&value->cid)) {
			inherited_sparse_num++;
		}
		fprintf(fp, "%ld %d\n", value->cid, value->size);
	}
	fclose(fp);

	sdsfree(fname);
}

void har_check(struct chunk* c) {
	if (c->size > 0 && CHECK_CHUNK_DUPLICATE(c))
		if (g_hash_table_lookup(inherited_sparse_containers, &c->id))
			c->flag |= CHUNK_SPARSE;
}
