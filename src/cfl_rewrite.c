#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "storage/containerstore.h"

/* First chunk with an ID that is different from the chunks in buffer. */
static struct chunk *wait;
static int buffer_size;

static int cfl_push(struct chunk *c) {
	if (c->size < 0) {
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
		struct chunk* c = NULL;
		int signal = recv_dedup_chunk(&c);

		if (signal == STREAM_END) {
			cfl_push(NULL);
			break;
		}

		if (!cfl_push(c))
			continue;

		/* The size of bufferred chunks is too small */
		if (buffer_size < destor.rewrite_cfl_usage_threshold * CONTAINER_SIZE)
			out_of_order = 1;

		struct chunk *c;
		while ((c = cfl_pop())) {
			if (out_of_order && c->size > 0 && CHECK_CHUNK_DUPLICATE(c))
				c->flag |= CHUNK_DUPLICATE;
			send_rewrite_chunk(c);
		}

		out_of_order = 0;
	}

	if (buffer_size < destor.rewrite_cfl_usage_threshold * CONTAINER_SIZE)
		out_of_order = 1;

	struct chunk *c;
	while ((c = cfl_pop())) {
		if (out_of_order && c->size > 0 && CHECK_CHUNK_DUPLICATE(c))
			c->flag |= CHUNK_DUPLICATE;
		send_rewrite_chunk(c);
	}

	term_rewrite_chunk_queue();

	return NULL;
}
