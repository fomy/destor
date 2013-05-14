#include "global.h"
#include "dedup.h"
#include "jcr.h"
#include "tools/sync_queue.h"

extern void send_fc_signal();
extern void send_fingerchunk(FingerChunk *fchunk, EigenValue *eigenvalue,
		BOOL update);
extern int recv_chunk_with_eigenvalue(Chunk **chunk);
extern ContainerId save_chunk(Chunk *chunk);
extern int rewriting_algorithm;

extern int32_t capping_T;
extern int32_t capping_segment_size;

typedef struct {
	ContainerId cid;
	int32_t length;
} ContainerRecord;

struct {
	Queue *chunk_queue;
	GSequence *container_record_seq; //
	int32_t size;
	BOOL has_new; //indicates whether there are new chunks in the segment
} cap_segment;

static void cap_segment_init() {
	cap_segment.chunk_queue = queue_new();
	cap_segment.container_record_seq = g_sequence_new(free);
	cap_segment.size = 0;
	cap_segment.has_new = FALSE;
}

static gint compare_cid(gconstpointer a, gconstpointer b, gpointer user_data) {
	return ((ContainerRecord*) a)->cid - ((ContainerRecord*) b)->cid;
}

static gint compare_length(gconstpointer a, gconstpointer b, gpointer user_data) {
	return ((ContainerRecord*) a)->length - ((ContainerRecord*) b)->length;
}

static BOOL cap_segment_push(Jcr *jcr, Chunk *chunk) {
	if ((cap_segment.size + chunk->length) > capping_segment_size) {
		return FALSE;
	}

	if (chunk->status & DUPLICATE) {
		ContainerRecord tmp_record;
		tmp_record.cid = chunk->container_id;
		GSequenceIter *iter = g_sequence_lookup(
				cap_segment.container_record_seq, &tmp_record, compare_cid,
				NULL );
		if (iter == NULL ) {
			ContainerRecord* record = malloc(sizeof(ContainerRecord));
			record->cid = chunk->container_id;
			record->length = chunk->length;
			g_sequence_insert_sorted(cap_segment.container_record_seq, record,
					compare_cid, NULL );
		} else {
			ContainerRecord* record = g_sequence_get(iter);
			if (record->cid != chunk->container_id) {
				dprint("error happens!");
			}
			record->length += chunk->length;
		}
	} else {
		cap_segment.has_new = TRUE;
	}
	queue_push(cap_segment.chunk_queue, chunk);
	cap_segment.size += chunk->length;
	return TRUE;
}

static Chunk* cap_segment_pop() {
	Chunk *chunk = queue_pop(cap_segment.chunk_queue);
	if (chunk == NULL ) {
		return NULL ;
	}
	cap_segment.size -= chunk->length;
	return chunk;
}

static void cap_segment_clear() {
	g_sequence_remove_range(
			g_sequence_get_begin_iter(cap_segment.container_record_seq),
			g_sequence_get_end_iter(cap_segment.container_record_seq));
	if (cap_segment.size != 0) {
		dprint("size != 0");
	}
	if (queue_size(cap_segment.chunk_queue) != 0) {
		dprint("queue is not empty!");
	}
	cap_segment.has_new = FALSE;
}

static void cap_segment_get_top() {
	int32_t length = g_sequence_get_length(cap_segment.container_record_seq);
	if (length <= capping_T)
		return;
	if (rewriting_algorithm == ECAP_REWRITING && cap_segment.has_new == FALSE
			&& length <= capping_T + 1) {
		return;
	}
	g_sequence_sort(cap_segment.container_record_seq, compare_length, NULL );
	/* remove extra records */
	int i = 0;
	for (; i < length - capping_T; ++i) {
		g_sequence_remove(
				g_sequence_get_begin_iter(cap_segment.container_record_seq));
	}
	length = g_sequence_get_length(cap_segment.container_record_seq);
	if (length != capping_T)
		dprint("length != capping_T");

	g_sequence_sort(cap_segment.container_record_seq, compare_cid, NULL );
}

void *cap_filter(void* arg) {
	Jcr *jcr = (Jcr*) arg;

	cap_segment_init();

	Chunk *chunk = 0, *remaining = 0;
	ContainerRecord tmp_record;
	BOOL stream_end = FALSE;

	while (TRUE) {
		chunk = NULL;
		int signal = recv_chunk_with_eigenvalue(&chunk);

		if (signal == STREAM_END) {
			free_chunk(chunk);
			chunk = NULL;
			stream_end = TRUE;
		}

		TIMER_DECLARE(b, e);
		TIMER_BEGIN(b);
		if (stream_end == TRUE || cap_segment_push(jcr, chunk) == FALSE) {
			/* segment is full */
			remaining = chunk;
			cap_segment_get_top();

			while (chunk = cap_segment_pop()) {
				TIMER_DECLARE(b1, e1);
				TIMER_BEGIN(b1);
				if (chunk->status & DUPLICATE) {
					tmp_record.cid = chunk->container_id;
					if (g_sequence_lookup(cap_segment.container_record_seq,
							&tmp_record, compare_cid, NULL ) == NULL ) {
						/* not in TOP_T */
						chunk->status |= OUT_OF_ORDER;
					}
				}

				BOOL update = FALSE;
				if (chunk->status & DUPLICATE) {
					if ((chunk->status & OUT_OF_ORDER)&&
					(chunk->status & NOT_IN_CACHE) ||
					chunk->status & SPARSE){
					chunk->container_id = save_chunk(chunk);
					update = TRUE;
					jcr->rewritten_chunk_amount += chunk->length;
					jcr->rewritten_chunk_count++;
				} else {
					jcr->dedup_size += chunk->length;
					++jcr->number_of_dup_chunks;
				}
			} else {
				chunk->container_id = save_chunk(chunk);
				update = TRUE;
			}

				FingerChunk *new_fchunk = (FingerChunk*) malloc(
						sizeof(FingerChunk));
				new_fchunk->container_id = chunk->container_id;
				new_fchunk->length = chunk->length;
				memcpy(&new_fchunk->fingerprint, &chunk->hash,
						sizeof(Fingerprint));
				TIMER_END(jcr->filter_time, b1, e1);
				send_fingerchunk(new_fchunk, chunk->eigenvalue, update);

				free_chunk(chunk);
			} //while

			cap_segment_clear();
			if (remaining)
				cap_segment_push(jcr, remaining);
		} else {
			TIMER_END(jcr->filter_time, b, e);
		}
		if (stream_end == TRUE)
			break;
	}

	save_chunk(NULL );

	send_fc_signal();

	queue_free(cap_segment.chunk_queue, free_chunk);
	g_sequence_free(cap_segment.container_record_seq);

	return NULL ;
}
