/*
 * backup.h
 *
 *  Created on: Dec 4, 2013
 *      Author: fumin
 */

#ifndef BACKUP_H_
#define BACKUP_H_

#include "destor.h"
#include "utils/sync_queue.h"

/*
 * CHUNK_FILE_START NORMAL_CHUNK... CHUNK_FILE_END
 */
void start_read_phase();
void stop_read_phase();

/*
 * Input: Raw data blocks
 * Output: Chunks
 */
void start_chunk_phase();
void stop_chunk_phase();
/* Input: Chunks
 * Output: Hashed Chunks.
 */
void start_hash_phase();
void stop_hash_phase();

void start_read_trace_phase();
void stop_read_trace_phase();

/*
 * Duplicate chunks are marked CHUNK_DUPLICATE
 */
void start_dedup_phase();
void stop_dedup_phase();
/*
 * Fragmented chunks are marked CHUNK_SPARSE, CHUNK_OUT_OF_ORDER or CHUNK_NOT_IN_CACHE.
 */
void start_rewrite_phase();
void stop_rewrite_phase();
/*
 * Determine which chunks are required to be written according to their flags.
 * All unique/rewritten chunks aggregate into containers.
 *
 * output: containers
 */
void start_filter_phase();
void stop_filter_phase();
/*
 * Write containers.
 */
void start_append_phase();
void stop_append_phase();

/* Output of read phase. */
SyncQueue* read_queue;
/* Output of chunk phase. */
SyncQueue* chunk_queue;
/* Output of hash phase. */
SyncQueue* hash_queue;
/* Output of trace phase. */
SyncQueue* trace_queue;
/* Output of dedup phase */
SyncQueue* dedup_queue;
/* Output of rewrite phase. */
SyncQueue* rewrite_queue;

#endif /* BACKUP_H_ */
