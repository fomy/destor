/*
 * jcr.h
 *
 *  Created on: Feb 15, 2012
 *      Author: fumin
 */

#ifndef Jcr_H_
#define Jcr_H_

#include "global.h"
#include "job/jobmanage.h"
#include "dedup.h"
#include "tools/sync_queue.h"
#include "storage/protos.h"

typedef struct jcr Jcr;
/* job control record */
struct jcr {
	int32_t job_id;
	char backup_path[200];
	char restore_path[200];
	int32_t file_num;
	int64_t dedup_size;
	int64_t job_size;
	int32_t chunk_num;
	int32_t number_of_dup_chunks;
	int32_t zero_chunk_count;
	int64_t zero_chunk_amount;
	int32_t rewritten_chunk_count;
	int64_t rewritten_chunk_amount;
	double time;

	int32_t sparse_container_num;
	int32_t inherited_sparse_num;
	int32_t total_container_num;

	JobVolume *job_volume;

	SyncQueue *completed_files_queue;
	SyncQueue *waiting_files_queue;

	ContainerCache *read_cache;
	OptimalContainerCache *read_opt_cache;
	Container *asm_buffer;
	BOOL enable_data_cache;
	int32_t read_cache_size;
	GHashTable* historical_sparse_containers;
	/*
	 * the time consuming of six dedup phase
	 */

	double read_time;
	double chunk_time;
	double name_time;
	double filter_time;
	double write_time;
	//double update_time;
	double test_time;

	double read_chunk_time;
	double write_file_time;
};

Jcr* new_write_jcr();
Jcr* new_read_jcr(int32_t rcs, BOOL edc);
void free_jcr(Jcr*);
#endif /* Jcr_H_ */
