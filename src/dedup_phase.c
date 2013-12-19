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
#include "index/index.h"
#include "backup.h"

static pthread_t dedup_t;

static int (*segmenting)(struct segment* s, struct chunk *c);

void send_segment(struct segment* s) {
	struct chunk* c;
	while ((c = g_queue_pop_head(s->chunks)))
		sync_queue_push(dedup_queue, c);

	s->chunk_num = 0;
	assert(s->features == NULL);
}

/*
 * Used by SiLo and Block Locality Caching.
 */
int segment_fixed(struct segment* s, struct chunk * c) {

	if (c == NULL)
		/* STREAM_END */
		return 1;

	g_queue_push_tail(s->chunks, c);
	if (CHECK_CHUNK(c,
			CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
		/* FILE_END */
		return 0;

	/* a normal chunk */
	s->chunk_num++;

	if (s->chunk_num == destor.index_segment_algorithm[1])
		/* segment boundary */
		return 1;

	return 0;
}

/*
 * Used by Extreme Binning.
 */
int segment_file_defined(struct segment* s, struct chunk *c) {
	/*
	 * For file-defined segmenting,
	 * the end is not a new segment.
	 */
	if (c == NULL)
		return 0;

	g_queue_push_tail(s->chunks, c);
	if (CHECK_CHUNK(c, CHUNK_FILE_END)) {
		return 1;
	} else if (CHECK_CHUNK(c, CHUNK_FILE_START)) {
		return 0;
	} else {
		/* a normal chunk */
		s->chunk_num++;
		return 0;
	}
}

/*
 * Used by Sparse Index.
 */
int segment_content_defined(struct segment* s, struct chunk *c) {
	if (c == NULL)
		return 1;

	g_queue_push_tail(s->chunks, c);
	if (CHECK_CHUNK(c,
			CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
		return 0;

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
		if (destor.simulation_level != SIMULATION_ALL)
			c = sync_queue_pop(hash_queue);
		else
			c = sync_queue_pop(trace_queue);

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		/* Add the chunk to the segment. */
		int success = segmenting(s, c);

		/* For indexes exploiting logical locality */
		if (destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY)
			s->features = featuring(
					(!c || CHECK_CHUNK(c, CHUNK_FILE_START)
							|| CHECK_CHUNK(c, CHUNK_FILE_END)) ?
					NULL :
																	&c->fp,
					success);

		TIMER_END(1, jcr.dedup_time);

		if (success) {
			/* Each redundant chunk will be marked. */
			index_lookup(s);

			/* Send chunks in the segment to the next phase.
			 * The segment will be cleared. */
			send_segment(s);
		}

		if (c == NULL)
			break;
	}
	sync_queue_term(dedup_queue);
	free_segment(s, free_chunk);
	return NULL;
}

void start_dedup_phase() {
	dedup_queue = sync_queue_new(1000);
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
