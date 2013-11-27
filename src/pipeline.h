/*
 * pipeline.h
 *
 *  Created on: Nov 18, 2013
 *      Author: fumin
 */

#ifndef PIPELINE_H_
#define PIPELINE_H_

#include "destor.h"

/*
 * Input: backup files
 * Output: Raw data blocks
 * We consider the file name as the first block of the file,
 * with a negative size of the filename.
 * -----------------------
 * - (-n) -  filename    -
 * -----------------------
 * -----------------------
 * - (n) -    block      -
 * -----------------------
 */
int start_read_phase();

/*
 * Input: Raw data blocks
 * Output: Chunks
 * We consider the file name as the first chunk of the file.
 */
int start_chunk_phase();
/* Input: Chunks
 * Output: Hashed Chunks.
 * The file name is the first chunk.
 */
int start_hash_phase();

int start_read_trace_phase();

/*
 * Duplicate chunks are marked CHUNK_DUPLICATE
 */
int start_dedup_phase();
/*
 * Fragmented chunks are marked CHUNK_SPARSE, CHUNK_OUT_OF_ORDER or CHUNK_NOT_IN_CACHE.
 */
int start_rewrite_phase();
/*
 * Determine which chunks are required to be written according to their flags.
 * All unique/rewritten chunks aggregate into containers.
 *
 * output: containers
 */
int start_filter_phase();
/*
 * Write containers.
 */
int start_append_phase();


#endif /* PIPELINE_H_ */
