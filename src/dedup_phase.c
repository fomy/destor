/*
 * In the phase,
 * we aggregate chunks into segments,
 * and deduplicate each segment with its similar segments.
 * All redundant chunks are identified.
 * Other fingerprint indexes who have no segment (e.g., DDFS, Sampled Index)
 * can be regard as a fixed segment size of 1.
 */
#include "destor.h"
#include "jcr.h"
#include "tools/sync_queue.h"
#include "index/index.h"

extern int recv_hashed_chunk(struct chunk **ck);
extern int recv_trace_chunk(struct chunk **ck);

/* output of segment_thread */
static SyncQueue* dedup_chunk_queue;
static pthread_t dedup_t;

void send_dedup_chunks(struct segment* s) {
	struct chunk* c = NULL;
	while ((c = g_queue_pop_head(s->chunks)) != NULL)
		sync_queue_push(dedup_chunk_queue, c);
	s->chunk_num = 0;
	g_sequence_remove_range(g_sequence_get_begin_iter(s->features),
			g_sequence_get_end_iter(s->features));
}

void term_dedup_chunk_queue() {
	sync_queue_term(dedup_chunk_queue);
}

int recv_dedup_chunk(struct chunk** c) {
	*c = sync_queue_pop(dedup_chunk_queue);
	if (*c == NULL)
		return STREAM_END;
	if ((*c)->size < 0)
		return FILE_END;
	return (*c)->size;
}

static int (*segmenting)(struct segment* s, struct chunk *c);

/*
 * Used by SiLo and Block Locality Caching.
 */
int segment_fixed(struct segment* s, struct chunk * c) {

	if (c == NULL) {
		/* STREAM_END */
		return 1;
	} else if (c->size < 0) {
		/* FILE_END */
		g_queue_push_tail(s->chunks, c);
		return 0;
	}
	/* a chunk */
	g_queue_push_tail(s->chunks, c);
	s->chunk_num++;

	if (s->chunk_num == destor.index_segment_algorithm[1]) {
		/* segment boundary */
		return 1;
	}
	return 0;
}

/*
 * Used by Extreme Binning.
 */
int segment_file_defined(struct segment* s, struct chunk *c) {
	if (c == NULL) {
		return 1;
	} else if (c->size < 0) {
		g_queue_push_tail(s->chunks, c);
		return 1;
	}
	/* a chunk */
	g_queue_push_tail(s->chunks, c);
	s->chunk_num++;
	return 0;
}

/*
 * Used by Sparse Index.
 */
int segment_content_defined(struct segment* s, struct chunk *c) {
	if (c == NULL) {
		return 1;
	} else if (c->size < 0) {
		g_queue_push_tail(s->chunks, c);
		return 0;
	}
	g_queue_push_tail(s->chunks, c);
	s->chunk_num++;

	/* Avoid too small segment. */
	if (s->chunk_num < 2 * destor.index_feature_method[1])
		return 0;

	if ((*((int*) (&c->fp))) % destor.index_segment_algorithm[1] == 0)
		return 1;
	return 0;
}

void *dedup_thread(void *arg) {
	struct segment* s = new_segment();
	while (1) {
		struct chunk *c = NULL;
		int signal;
		if (simulation_level == SIMULATION_ALL)
			signal = recv_trace_chunk(&c);
		else
			signal = recv_hashed_chunk(&c);

		/* Add the chunk to the segment. */
		int success = segmenting(s, c);

		/* For indexes exploiting logical locality */
		if (destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY)
			s->features = featuring(signal < 0 ? NULL : &c->fp, success);

		if (success) {
			/* Each redundant chunk will be marked. */
			index_lookup(s);

			/* Send chunks in the segment to the next phase.
			 * The segment will be cleared. */
			send_dedup_chunks(s);
		}

		if (signal == STREAM_END)
			break;
	}
	term_dedup_chunk_queue();
	free_segment(s, free_chunk);
	return NULL;
}

void start_dedup_phase() {
	dedup_chunk_queue = sync_queue_new(1000);
	switch (destor.index_segment_algorithm[0]) {
	case INDEX_SEGMENT_FIXED:
		segmenting = segment_fixed;
		break;
	case INDEX_SEGMENT_CONTENT_DEFINED:
		segmenting = segment_content_defined;
		break;
	case INDEX_SEGMENT_FILE_DEFINED:
		segmenting = segment_file_defined;
		break;
	default:
		fprintf(stderr, "Invalid segment algorithm!\n");
		exit(1);
	}

	pthread_create(&dedup_t, NULL, dedup_thread, NULL);
}

void stop_dedup_phase() {
	pthread_join(dedup_t, NULL);
}
