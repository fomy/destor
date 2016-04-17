#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "backup.h"

static int64_t chunk_num;

static GHashTable *top;

static void cap_segment_get_top() {

	/* Descending order */
	g_sequence_sort(rewrite_buffer.container_record_seq,
			g_record_descmp_by_length, NULL);

	int length = g_sequence_get_length(rewrite_buffer.container_record_seq);
	int32_t num = length > destor.rewrite_capping_level ?
					destor.rewrite_capping_level : length, i;
	GSequenceIter *iter = g_sequence_get_begin_iter(
			rewrite_buffer.container_record_seq);
	for (i = 0; i < num; i++) {
		assert(!g_sequence_iter_is_end(iter));
		struct containerRecord* record = g_sequence_get(iter);
		struct containerRecord* r = (struct containerRecord*) malloc(
				sizeof(struct containerRecord));
		memcpy(r, record, sizeof(struct containerRecord));
		r->out_of_order = 0;
		g_hash_table_insert(top, &r->cid, r);
		iter = g_sequence_iter_next(iter);
	}

	VERBOSE("Rewrite phase: Select Top-%d in %d containers", num, length);

	g_sequence_sort(rewrite_buffer.container_record_seq, g_record_cmp_by_id, NULL);
}

/*
 * We first assemble a fixed-sized buffer of pending chunks.
 * Then, counting container utilization in the buffer and sorting.
 * The pending chunks in containers of most low utilization are fragmentation.
 * The main drawback of capping,
 * is that capping overlook the relationship of consecutive buffers.
 */
void *cap_rewrite(void* arg) {
	top = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);

	while (1) {
		struct chunk *c = sync_queue_pop(dedup_queue);

		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		if (!rewrite_buffer_push(c)) {
			TIMER_END(1, jcr.rewrite_time);
			continue;
		}

		cap_segment_get_top();

		while ((c = rewrite_buffer_pop())) {
			if (!CHECK_CHUNK(c,	CHUNK_FILE_START) 
					&& !CHECK_CHUNK(c, CHUNK_FILE_END)
					&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) 
					&& !CHECK_CHUNK(c, CHUNK_SEGMENT_END)
					&& CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
				if (g_hash_table_lookup(top, &c->id) == NULL) {
					/* not in TOP */
					SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
					VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
							chunk_num, c->id);
				}
				chunk_num++;
			}
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_push(rewrite_queue, c);
			TIMER_BEGIN(1);
		}

		g_hash_table_remove_all(top);

	}

	cap_segment_get_top();

	struct chunk *c;
	while ((c = rewrite_buffer_pop())) {
		if (!CHECK_CHUNK(c,	CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
				&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) && !CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
			if (g_hash_table_lookup(top, &c->id) == NULL) {
				/* not in TOP */
				SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
				VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
						chunk_num, c->id);
			}
			chunk_num++;
		}
		sync_queue_push(rewrite_queue, c);
	}

	g_hash_table_remove_all(top);

	sync_queue_term(rewrite_queue);

	return NULL;
}
