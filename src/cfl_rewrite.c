#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "storage/containerstore.h"
#include "backup.h"

static int64_t chunk_num;

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
	/*
	 * A chunk with an ID that is different from the chunks in buffer,
	 * or a NULL pointer,
	 * indicates a segment boundary (return 1).
	 */
	containerid last_id = TEMPORARY_ID;
	int buffer_full = 0;
	while (1) {
		struct chunk* c = sync_queue_pop(dedup_queue);
		if (c == NULL) {
			/* The end */
			break;
		}

		if ((last_id != TEMPORARY_ID && last_id != c->id) || buffer_full == 1) {
			/* judge */
			int out_of_order = rewrite_buffer.size
					< destor.rewrite_cfl_usage_threshold
							* (CONTAINER_SIZE - CONTAINER_META_SIZE);

			struct chunk* bc;
			while ((bc = rewrite_buffer_pop())) {
				if (CHECK_CHUNK(bc,	CHUNK_FILE_START) || CHECK_CHUNK(bc, CHUNK_FILE_END)
						|| CHECK_CHUNK(bc, CHUNK_SEGMENT_START)
						|| CHECK_CHUNK(bc, CHUNK_SEGMENT_END)) {
					sync_queue_push(rewrite_queue, bc);
					continue;
				}

				if (out_of_order && bc->id != TEMPORARY_ID) {
					assert(CHECK_CHUNK(bc, CHUNK_DUPLICATE));
					SET_CHUNK(bc, CHUNK_OUT_OF_ORDER);
					VERBOSE(
							"Rewrite phase: %lldth chunk is in out-of-order container %lld",
							chunk_num, bc->id);
				}
				chunk_num++;
				sync_queue_push(rewrite_queue, bc);
			}
			buffer_full = 0;
		}

		last_id = c->id;
		if (rewrite_buffer_push(c)) {
			buffer_full = 1;
		}
	}

	int out_of_order = rewrite_buffer.size
			< destor.rewrite_cfl_usage_threshold * (CONTAINER_SIZE - CONTAINER_META_SIZE);

	struct chunk* bc;
	while ((bc = rewrite_buffer_pop())) {
		if (CHECK_CHUNK(bc,	CHUNK_FILE_START) || CHECK_CHUNK(bc, CHUNK_FILE_END)
				|| CHECK_CHUNK(bc, CHUNK_SEGMENT_START)
				|| CHECK_CHUNK(bc, CHUNK_SEGMENT_END)) {
			sync_queue_push(rewrite_queue, bc);
			continue;
		}

		if (out_of_order && bc->id != TEMPORARY_ID) {
			assert(CHECK_CHUNK(bc, CHUNK_DUPLICATE));
			SET_CHUNK(bc, CHUNK_OUT_OF_ORDER);
			VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
					chunk_num, bc->id);
		}
		chunk_num++;
		sync_queue_push(rewrite_queue, bc);
	}
	buffer_full = 0;

	sync_queue_term(rewrite_queue);
	return NULL;
}
