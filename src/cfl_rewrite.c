#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "storage/containerstore.h"
#include "backup.h"

/* First chunk with an ID that is different from the chunks in buffer. */
static struct chunk *wait;
static int buffer_size;

static int cfl_push(struct chunk *c) {
	if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
		rewrite_buffer_push(c);
		return 0;
	}

	if (!wait) {
		wait = c;
		return 1;
	}

	rewrite_buffer_push(wait);
	buffer_size += wait->size;

	if (c == NULL || wait->id != c->id || c->id == TEMPORARY_ID) {
		wait = c;
		return 1;
	}
	wait = c;
	return 0;
}

static struct chunk* cfl_pop() {
	struct chunk *c = rewrite_buffer_pop();
	if (c->size > 0)
		buffer_size -= c->size;
	return c;
}

/* --------------------------------------------------------------------------*/
/**
 * @Synopsis  Assuring Demanded Read Performance of Data Deduplication Storage
 *            with Backup Datasets. In MASCOTS'12.
 *
 * @Param arg
 *
 * @Returns   
 */
/* ----------------------------------------------------------------------------*/
void *cfl_rewrite(void* arg) {

	int out_of_order = 0;

	while (1) {
		struct chunk* c = sync_queue_pop(dedup_queue);

		if (c == NULL) {
			cfl_push(NULL);
			break;
		}

		if (!cfl_push(c))
			continue;

		/* The size of bufferred chunks is too small */
		if (buffer_size < destor.rewrite_cfl_usage_threshold * CONTAINER_SIZE)
			out_of_order = 1;

		while ((c = cfl_pop())) {
			if (out_of_order
					&& !(CHECK_CHUNK(c, CHUNK_FILE_START)
							|| CHECK_CHUNK(c, CHUNK_FILE_END))
					&& CHECK_CHUNK(c, CHUNK_DUPLICATE))
				SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
			sync_queue_push(rewrite_queue, c);
		}

		out_of_order = 0;
	}

	if (buffer_size < destor.rewrite_cfl_usage_threshold * CONTAINER_SIZE)
		out_of_order = 1;

	struct chunk *c;
	while ((c = cfl_pop())) {
		if (out_of_order
				&& !(CHECK_CHUNK(c, CHUNK_FILE_START)
						|| CHECK_CHUNK(c, CHUNK_FILE_END))
				&& CHECK_CHUNK(c, CHUNK_DUPLICATE))
			SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
		sync_queue_push(rewrite_queue, c);
	}

	sync_queue_term(rewrite_queue);

	return NULL;
}
