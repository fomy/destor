#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "storage/containerstore.h"
#include "backup.h"

static struct chunk *wait;
static int buffer_size;
static int64_t chunk_num;

/*
 * A chunk with an ID that is different from the chunks in buffer,
 * or a NULL pointer,
 * indicates a segment boundary (return 1).
 */
static int cfl_push(struct chunk *c) {

	/* wait == NULL indicates the beginning. */
	if (!wait) {
		wait = c;
		return c == NULL ? 1 : 0;
	}

	if (c == NULL) {
		/* The end */
		assert(CHECK_CHUNK(wait, CHUNK_FILE_END));
		rewrite_buffer_push(wait);
		wait = 0;
		return 1;
	}

	rewrite_buffer_push(wait);

	if (!CHECK_CHUNK(wait,
			CHUNK_FILE_START) && !CHECK_CHUNK(wait, CHUNK_FILE_END)) {
		buffer_size += wait->size;
		if (wait->id != c->id || c->id == TEMPORARY_ID
				|| rewrite_buffer.num >= destor.rewrite_algorithm[1]) {
			wait = c;
			return 1;
		}
	}

	wait = c;

	return 0;
}

static struct chunk* cfl_pop() {
	struct chunk *c = rewrite_buffer_pop();
	if (c
			&& !CHECK_CHUNK(c,
					CHUNK_FILE_START) && !CHECK_CHUNK(c,CHUNK_FILE_END))
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
			/* push NULL is required.*/
			cfl_push(NULL);
			break;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		if (!cfl_push(c)) {
			TIMER_END(1, jcr.rewrite_time);
			continue;
		}

		/* The size of bufferred chunks is too small */
		if (buffer_size < destor.rewrite_cfl_usage_threshold * CONTAINER_SIZE)
			out_of_order = 1;

		while ((c = cfl_pop())) {
			if (!CHECK_CHUNK(c,
					CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)) {
				if (out_of_order
						&& CHECK_CHUNK(c,
								CHUNK_DUPLICATE) && c->id != TEMPORARY_ID) {
					SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
					VERBOSE(
							"Rewrite phase: %lldth chunk is in out-of-order container %lld",
							chunk_num, c->id);
				}
				chunk_num++;
			}
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_push(rewrite_queue, c);
			TIMER_BEGIN(1);
		}

		out_of_order = 0;

	}

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	if (buffer_size < destor.rewrite_cfl_usage_threshold * CONTAINER_SIZE)
		out_of_order = 1;

	struct chunk *c;
	while ((c = cfl_pop())) {
		if (!(CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))) {
			if (out_of_order && CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
				SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
				VERBOSE(
						"Rewrite phase: %lldth chunk is in out-of-order container %lld",
						chunk_num, c->id);
			}
			chunk_num++;
		}
		TIMER_END(1, jcr.rewrite_time);
		sync_queue_push(rewrite_queue, c);
		TIMER_BEGIN(1);
	}

	sync_queue_term(rewrite_queue);

	return NULL;
}
