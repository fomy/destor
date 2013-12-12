/*
 * append_phase.c
 *
 *  Created on: Nov 17, 2013
 *      Author: fumin
 */

#include "destor.h"
#include "jcr.h"
#include "storage/containerstore.h"
#include "backup.h"

static pthread_t append_t;

static void* append_thread(void *arg) {

	while (1) {
		struct container *c = sync_queue_pop(container_queue);
		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		write_container(c);

		TIMER_END(1, jcr.write_time);

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
