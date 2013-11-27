#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"

static int cap_segment_push(struct chunk *c) {

	rewrite_buffer_push(c);

	if (rewrite_buffer.num == destor.rewrite_algorithm[1])
		return 1;
	return 0;
}

static struct chunk* cap_segment_pop() {
	return rewrite_buffer_pop();
}

static GHashTable *top;

static void cap_segment_get_top() {

	/* Descending order */
	g_sequence_sort(rewrite_buffer.container_record_seq,
			g_record_descmp_by_length, NULL);

	int length = g_sequence_get_length(rewrite_buffer.container_record_seq);
	int32_t num =
			length > destor.rewrite_capping_level ?
					destor.rewrite_capping_level : length, i;
	GSequenceIter *iter = g_sequence_get_begin_iter(
			rewrite_buffer.container_record_seq);
	for (i = 0; i < num; i++) {
		assert(!g_sequence_iter_is_end(iter));
		struct containerRecord* record = g_sequence_get(iter);
		struct containerRecord* r = (struct containerRecord*) malloc(
				sizeof(struct containerRecord));
		memcpy(r, record, sizeof(struct containerRecord));
		g_hash_table_insert(top, &r->cid, r);
		iter = g_sequence_iter_next(iter);
	}

	g_sequence_sort(rewrite_buffer.container_record_seq, g_record_cmp_by_id,
	NULL);
}

void *cap_rewrite(void* arg) {
	top = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);

	while (1) {
		struct chunk *c = NULL;
		int signal = recv_dedup_chunk(&c);

		if (signal == STREAM_END) {
			break;
		}

		if (!cap_segment_push(c))
			continue;

		cap_segment_get_top();

		while ((c = cap_segment_pop())) {
			if (c->size > 0 && CHECK_CHUNK_DUPLICATE(c)) {
				if (g_hash_table_lookup(top, &c->id) == NULL) {
					/* not in TOP */
					c->flag |= CHUNK_OUT_OF_ORDER;
				}
			}
			send_rewrite_chunk(c);
		}

		g_hash_table_remove_all(top);
	}

	cap_segment_get_top();

	struct chunk *c;
	while ((c = cap_segment_pop())) {
		if (c->size > 0 && CHECK_CHUNK_DUPLICATE(c)) {
			if (g_hash_table_lookup(top, &c->id) == NULL) {
				/* not in TOP */
				c->flag |= CHUNK_OUT_OF_ORDER;
			}
		}
		send_rewrite_chunk(c);
	}

	g_hash_table_remove_all(top);

	term_rewrite_chunk_queue();

	g_queue_free(rewrite_buffer.chunk_queue, free_chunk);
	g_sequence_free(rewrite_buffer.container_record_seq);

	return NULL;
}
