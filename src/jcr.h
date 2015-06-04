/*
 * jcr.h
 *
 *  Created on: Feb 15, 2012
 *      Author: fumin
 */

#ifndef Jcr_H_
#define Jcr_H_

#include "destor.h"
#include "recipe/recipestore.h"

#define JCR_STATUS_INIT 1
#define JCR_STATUS_RUNNING 2
#define JCR_STATUS_DONE 3

/* job control record */
struct jcr{
	int32_t id;
	/*
	 * The path of backup or restore.
	 */
	sds path;

    int status;

	int32_t file_num;
	int64_t data_size;
	int64_t unique_data_size;
	int32_t chunk_num;
	int32_t unique_chunk_num;
	int32_t zero_chunk_num;
	int64_t zero_chunk_size;
	int32_t rewritten_chunk_num;
	int64_t rewritten_chunk_size;

	int32_t sparse_container_num;
	int32_t inherited_sparse_num;
	int32_t total_container_num;

	struct backupVersion* bv;

	double total_time;
	/*
	 * the time consuming of six dedup phase
	 */
	double read_time;
	double chunk_time;
	double hash_time;
	double dedup_time;
	double rewrite_time;
	double filter_time;
	double write_time;

	double read_recipe_time;
	double read_chunk_time;
	double write_chunk_time;

	int32_t read_container_num;
};

extern struct jcr jcr;

void init_jcr(char *path);
void init_backup_jcr(char *path);
void init_restore_jcr(int revision, char *path);

#endif /* Jcr_H_ */
