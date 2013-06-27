#include "global.h"
#include "jcr.h"
#include "storage/protos.h"
#include "tools/sync_queue.h"

/* container queue */
static SyncQueue* container_queue;

/*
 * Do NOT use write_buffer as a part of index,
 * such as using container_contains() to filter duplicates further.
 * Our statistics only measure the efficiency of index and rewriting algorithms.
 * Even some duplicates may be found while adding to container,
 * we ignore it.
 * So the container volume may seems inconsistency with statistics.
 */
static Container *write_buffer = NULL;

static pthread_t append_t;

/*
 * chunk==NULL indicates STREAM_END 
 * */
ContainerId save_chunk(Chunk *chunk) {
	if (chunk == NULL) {
		Container *container = write_buffer;
		if (seal_container(container) != TMP_CONTAINER_ID) {
			sync_queue_push(container_queue, container);
		}

		Container *signal = container_new_meta_only();
		signal->id = STREAM_END;
		sync_queue_push(container_queue, signal);
		return TMP_CONTAINER_ID;
	}
	while(write_buffer == NULL)
		sleep(1);
	while (container_add_chunk(write_buffer, chunk) == CONTAINER_FULL) {
		Container *container = write_buffer;
		seal_container(container);
		sync_queue_push(container_queue, container);
		write_buffer = create_container();
	}
	chunk->container_id = write_buffer->id;
	return write_buffer->id;
}

/*
 * Handle containers in container_queue.
 * When a container buffer is full, we push it into container_queue.
 */
static void* append_thread(void *arg) {
	Jcr* jcr = (Jcr*) arg;
	while (TRUE) {
		Container *container = sync_queue_pop(container_queue);
		if (container->id == STREAM_END) {
			/* backup job finish */
			container_free_full(container);
			break;
		}
		TIMER_DECLARE(b, e);
		TIMER_BEGIN(b);
		append_container(container);
		TIMER_END(jcr->write_time, b, e);
		container_free_full(container);
	}
}

int start_append_phase(Jcr *jcr) {
	write_buffer = create_container();
	container_queue = sync_queue_new(100);
	pthread_create(&append_t, NULL, append_thread, jcr);
}

void stop_append_phase() {
	pthread_join(append_t, NULL);
}
