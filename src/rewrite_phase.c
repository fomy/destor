/*
 * In the phase,
 * we mark the chunks required to be rewriting.
 */
#include "destor.h"
#include "jcr.h"
#include "tools/sync_queue.h"
#include "rewrite_phase.h"

extern int recv_dedup_chunk(struct chunk **chunk);

static pthread_t rewrite_t;
static SyncQueue* rewrite_chunk_queue;

void send_rewrite_chunk(struct chunk* c) {
	if (destor.rewrite_enable_har)
		har_check(c);

	sync_queue_push(rewrite_chunk_queue, c);
}

void term_rewrite_chunk_queue() {
	sync_queue_term(rewrite_chunk_queue);
}

int recv_rewrite_chunk(struct chunk** c) {
	*c = sync_queue_pop(rewrite_chunk_queue);
	if ((*c) == NULL)
		return STREAM_END;
	if ((*c)->size < 0)
		return FILE_END;
	return (*c)->size;
}

/* Descending order */
gint g_record_descmp_by_length(struct containerRecord* a,
		struct containerRecord* b, gpointer user_data) {
	return b->size - a->size;
}

gint g_record_cmp_by_id(struct containerRecord* a, struct containerRecord* b,
		gpointer user_data) {
	return a->cid - b->cid;
}

static void init_rewrite_buffer() {
	rewrite_buffer.chunk_queue = g_queue_new();
	rewrite_buffer.container_record_seq = g_sequence_new(free);
	rewrite_buffer.num = 0;
}

void rewrite_buffer_push(struct chunk* c) {
	g_queue_push_tail(rewrite_buffer.chunk_queue, c);

	if (c->size < 0)
		return;

	if (CHECK_CHUNK_DUPLICATE(c)) {
		struct containerRecord tmp_record;
		tmp_record.cid = c->id;
		GSequenceIter *iter = g_sequence_lookup(
				rewrite_buffer.container_record_seq, &tmp_record,
				g_record_cmp_by_id,
				NULL);
		if (iter == NULL) {
			struct containerRecord* record = malloc(
					sizeof(struct containerRecord));
			record->cid = c->id;
			record->size = c->size;
			g_sequence_insert_sorted(rewrite_buffer.container_record_seq,
					record, g_record_cmp_by_id, NULL);
		} else {
			struct containerRecord* record = g_sequence_get(iter);
			assert(record->cid == c->id);
			record->size += c->size;
		}
	}

	rewrite_buffer.num++;
}

struct chunk* rewrite_buffer_pop() {
	struct chunk* c = g_queue_pop_tail(rewrite_buffer.chunk_queue);

	if (c->size > 0) {
		/* A normal chunk */
		if (CHECK_CHUNK_DUPLICATE(c)) {
			GSequenceIter *iter = g_sequence_lookup(
					rewrite_buffer.container_record_seq, &c->id);
			assert(iter);
			struct containerRecord* record = g_sequence_get(iter);
			record->size -= c->size;
			if (record->size == 0)
				g_sequence_remove(iter);
		}
		rewrite_buffer.num--;
	}

	return c;
}

/*
 * If rewrite is disable.
 */
static void* no_rewrite(void* arg) {
	while (1) {
		struct chunk* c;
		int signal = recv_dedup_chunk(&c);

		if (signal == STREAM_END) {
			break;
		}

		send_rewrite_chunk(c);

	}

	term_rewrite_chunk_queue();

	return NULL;
}

void start_rewrite_phase() {
	rewrite_chunk_queue = sync_queue_new(1000);
	init_rewrite_buffer();

	init_har();

	if (destor.rewrite_algorithm == REWRITE_NO) {
		pthread_create(&rewrite_t, NULL, no_rewrite, NULL);
	} else if (destor.rewrite_algorithm == REWRITE_CFL_SELECTIVE_DEDUPLICATION) {
		pthread_create(&rewrite_t, NULL, cfl_rewrite, jcr);
	} else if (destor.rewrite_algorithm == REWRITE_CONTEXT_BASED) {
		pthread_create(&rewrite_t, NULL, cbr_rewrite, jcr);
	} else if (destor.rewrite_algorithm == REWRITE_CAPPING) {
		pthread_create(&rewrite_t, NULL, cap_rewrite, jcr);
	} else {
		dprint("Invalid rewrite algorithm\n");
		exit(1);
	}

	close_har();
}

void stop_rewrite_phase() {
	pthread_join(rewrite_t, NULL);
}
