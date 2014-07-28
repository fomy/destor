/*
 * delta_phase.c
 *
 *  Created on: Jul 28, 2014
 *      Author: fumin
 */
#include "destor.h"
#include "jcr.h"
#include "backup.h"

static pthread_t delta_t;

void *delta_thread(void *arg) {
    while (1) {
        struct chunk* c = sync_queue_pop(rewrite_queue);

        if (c == NULL)
            /* backup job finish */
            break;

        if(CHECK_CHUNK(c, CHUNK_FILE_START) ||
        		CHECK_CHUNK(c, CHUNK_FILE_END) ||
        		CHECK_CHUNK(c, CHUNK_SEGMENT_START) ||
        		CHECK_CHUNK(c, CHUNK_SEGMENT_END))
        	sync_queue_push(delta_queue, c);

        if(CHECK_CHUNK(c, CHUNK_DUPLICATE))
        	sync_queue_push(delta_queue, c);

        /* do delta compression */

        sync_queue_push(delta_queue, c);
    }

    sync_queue_term(delta_queue);

	return NULL;
}

void start_delta_phase() {
    delta_queue = sync_queue_new(1000);

    pthread_create(&delta_t, NULL, delta_thread, NULL);

}

void stop_rewrite_phase() {
    pthread_join(delta_t, NULL);
}
