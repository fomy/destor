/*
 * append_phase.c
 *
 *  Created on: Nov 17, 2013
 *      Author: fumin
 */

#include "destor.h"
#include "jcr.h"
#include "storage/containerstore.h"
#include "tools/sync_queue.h"

extern int recv_container(struct container** c);

static pthread_t append_t;

static void* append_thread(void *arg) {

	while (1) {
		struct container *c;
		int sig = recv_container(&c);
		if (sig == STREAM_END)
			break;

		TIMER_DECLARE(b, e);
		TIMER_BEGIN(b);

		write_container(c);

		TIMER_END(jcr->write_time, b, e);

		free_container(c);
	}

	return NULL;
}

void start_append_phase() {
	pthread_create(&append_t, NULL, append_thread, NULL);
}

void stop_append_phase() {
	pthread_join(append_t, NULL);
}
