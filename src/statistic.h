#ifndef _STATISTIC_H_
#define _STATISTIC_H_
#include "global.h"

/*
 * 0xabcdabcd
 */
typedef struct destor_stat {
	int64_t number_of_chunks;

	int64_t number_of_dup_chunks;

	int64_t data_amount;
	int64_t consumed_capacity;
	int64_t saved_capacity;

	int32_t zero_chunk_count;
	int64_t zero_chunk_amount;

	int32_t rewritten_chunk_count;
	int64_t rewritten_chunk_amount;

	int32_t sparse_chunk_count;
	int64_t sparse_chunk_amount;

	int simulation_level;
} DestorStat;

extern int init_destor_stat();
extern int free_destor_stat();
extern void print_stat_server();
extern int init_destor_stat();
extern int free_destor_stat();
extern void print_destor_stat();
extern int update_destor_stat();

#endif
