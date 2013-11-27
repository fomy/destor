/*
 * rewrite_phase.h
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */

#include "destor.h"

#ifndef REWRITE_PHASE_H_
#define REWRITE_PHASE_H_

struct containerRecord {
	containerid cid;
	int32_t size;
};

struct {
	GQueue *chunk_queue;
	GSequence *container_record_seq; //
	int num;
} rewrite_buffer;

void* cfl_rewrite(void* arg);
void* cbr_rewrite(void* arg);
void* cap_rewrite(void* arg);

void init_har();
void close_har();
void har_monitor_update(containerid id, int32_t size);
void har_check(struct chunk* c);

void send_rewrite_chunk(struct chunk* c);

void term_rewrite_chunk_queue();

/* For sorting container records. */
gint g_record_descmp_by_length(struct containerRecord* a,
		struct containerRecord* b, gpointer user_data)
gint g_record_cmp_by_id(struct containerRecord* a, struct containerRecord* b,
		gpointer user_data);

void rewrite_buffer_push(struct chunk* c);
struct chunk* rewrite_buffer_pop();

#endif /* REWRITE_PHASE_H_ */
