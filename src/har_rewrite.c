/*
 * har_rewrite.c
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */

#include "destor.h"
#include "rewrite_phase.h"
#include "storage/containerstore.h"
#include "jcr.h"
#include "cma.h"

static GHashTable *container_utilization_monitor;
static GHashTable *inherited_sparse_containers;

void init_har() {

	/* Monitor the utilizations of containers */
	container_utilization_monitor = g_hash_table_new_full(
			g_int64_hash, g_int64_equal, NULL, free);

	/* IDs of inherited sparse containers */
	inherited_sparse_containers = g_hash_table_new_full(g_int64_hash,
			g_int64_equal, NULL, free);

	/* The first backup doesn't have inherited sparse containers. */
	if (jcr.id > 0) {

		sds fname = sdsdup(destor.working_directory);
		fname = sdscat(fname, "recipes/bv");
		char s[20];
		sprintf(s, "%d", jcr.id - 1);
		fname = sdscat(fname, s);
		fname = sdscat(fname, ".sparse");

		FILE* sparse_file = fopen(fname, "r");

		if (sparse_file) {
			char buf[128];
			while (fgets(buf, 128, sparse_file) != NULL) {
				struct containerRecord *record =
						(struct containerRecord*) malloc(
								sizeof(struct containerRecord));
				sscanf(buf, "%lld %d", &record->cid, &record->size);

				g_hash_table_insert(inherited_sparse_containers, &record->cid,
						record);
			}
			fclose(sparse_file);
		}

		sdsfree(fname);
	}

	NOTICE("Read %d inherited sparse containers",
			g_hash_table_size(inherited_sparse_containers));

}

void har_monitor_update(containerid id, int32_t size) {
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	struct containerRecord* record = g_hash_table_lookup(
			container_utilization_monitor, &id);
	if (record) {
		record->size += size;
	} else {

		record = (struct containerRecord*) malloc(
					sizeof(struct containerRecord));
		record->cid = id;
		record->size = 0;
		g_hash_table_insert(container_utilization_monitor,
				&record->cid, record);

		record->size += size;

	}
	TIMER_END(1, jcr.rewrite_time);
}

static gint g_record_cmp(struct containerRecord *a, struct containerRecord* b, gpointer user_data){
	return a->size - b->size;
}

void close_har() {
	sds fname = sdsdup(destor.working_directory);
	fname = sdscat(fname, "recipes/bv");
	char s[20];
	sprintf(s, "%d", jcr.id);
	fname = sdscat(fname, s);
	fname = sdscat(fname, ".sparse");

	FILE* fp = fopen(fname, "w");
	if (!fp) {
		fprintf(stderr, "Can not create sparse file");
		perror("The reason is");
		exit(1);
	}

	jcr.total_container_num = g_hash_table_size(container_utilization_monitor);

	GSequence *seq = g_sequence_new(NULL);
	int64_t total_size = 0;
	int64_t sparse_size = 0;

	/* collect sparse containers */
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, container_utilization_monitor);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct containerRecord* cr = (struct containerRecord*) value;
		total_size += cr->size;

		if((1.0*cr->size/(CONTAINER_SIZE - CONTAINER_META_SIZE))
				< destor.rewrite_har_utilization_threshold){
			/* It is sparse */
			if (inherited_sparse_containers
					&& g_hash_table_lookup(inherited_sparse_containers, &cr->cid))
				/* It is an inherited sparse container */
				jcr.inherited_sparse_num++;

			jcr.sparse_container_num++;
			sparse_size += cr->size;

			g_sequence_insert_sorted(seq, cr, g_record_cmp, NULL);
		}
	}

	/*
	 * If the sparse size is too large,
	 * we need to trim the sequence to control the rewrite ratio.
	 * We use sparse_size/total_size to estimate the rewrite ratio of next backup.
	 * However, the estimation is inaccurate (generally over-estimating), since:
	 * 	1. the sparse size is not an accurate indicator of utilization for next backup.
	 * 	2. self-references.
	 */
	while(destor.rewrite_har_rewrite_limit < 1
			&& sparse_size*1.0/total_size > destor.rewrite_har_rewrite_limit){
		/*
		 * The expected rewrite ratio exceeds the limit.
		 * We trim the last several records in the sequence.
		 * */
		GSequenceIter* iter = g_sequence_iter_prev(g_sequence_get_end_iter(seq));
		struct containerRecord* r = g_sequence_get(iter);
		VERBOSE("Trim sparse container %lld", r->cid);
		sparse_size -= r->size;
		g_sequence_remove(iter);
	}

	GSequenceIter* sparse_iter = g_sequence_get_begin_iter(seq);
	while(sparse_iter != g_sequence_get_end_iter(seq)){
		struct containerRecord* r = g_sequence_get(sparse_iter);
		fprintf(fp, "%lld %d\n", r->cid, r->size);
		sparse_iter = g_sequence_iter_next(sparse_iter);
	}
	fclose(fp);

	NOTICE("Record %d sparse containers, and %d of them are inherited",
			g_sequence_get_length(seq),	jcr.inherited_sparse_num);

	g_sequence_free(seq);
	sdsfree(fname);

	/* CMA: update the backup times in manifest */
	update_manifest(container_utilization_monitor);
}

void har_check(struct chunk* c) {
	if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
	&& CHECK_CHUNK(c, CHUNK_DUPLICATE))
		if (g_hash_table_lookup(inherited_sparse_containers, &c->id)) {
			SET_CHUNK(c, CHUNK_SPARSE);
			char code[41];
			hash2code(c->fp, code);
			code[40] = 0;
			VERBOSE("chunk %s in sparse container %ld", code, c->id);
		}
}
