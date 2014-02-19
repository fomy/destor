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
static int64_t chunk_num;
static int64_t segment_num;

/*
 * c == NULL indicates the end.
 * If a segment boundary is found, return 1;
 * else return 0.
 * If c == NULL, return 1.
 */
static int (*segmenting)(struct segment* s, struct chunk *c);

void send_segment(struct segment* s) {
	struct chunk* c;
	while ((c = g_queue_pop_head(s->chunks))) {
		if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)) {
			if (CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
				if (c->id == TEMPORARY_ID) {
					VERBOSE(
							"Dedup phase: %ldth chunk is identical to a unique chunk",
							chunk_num++);
				} else {
					VERBOSE(
							"Dedup phase: %ldth chunk is duplicate in container %lld",
							chunk_num++, c->id);
				}
			} else {
				VERBOSE("Dedup phase: %ldth chunk is unique", chunk_num++);
			}

		}
		sync_queue_push(dedup_queue, c);
	}

	s->chunk_num = 0;

}

/*
 * Used by SiLo and Block Locality Caching.
 */
int segment_fixed(struct segment* s, struct chunk * c) {

	if (c == NULL)
		/* The end of stream */
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
		return 1;

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
	static int max_segment_size = 102400;
	static int min_segment_size = 256;
	if (c == NULL)
		/* The end of stream */
		return 1;

	g_queue_push_tail(s->chunks, c);
	if (CHECK_CHUNK(c,
			CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
		return 0;

	s->chunk_num++;

	/* Avoid too small segment. */
	if (s->chunk_num < min_segment_size)
		return 0;
	if (s->chunk_num > max_segment_size)
		return 1;

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
			if (s->chunk_num > 0) {
				VERBOSE(
						"Dedup phase: the %lldth segment of %lld chunks paired with %d features",
						segment_num++, s->chunk_num,
						s->features ? g_hash_table_size(s->features) : 0);
				/* Each redundant chunk will be marked. */
				index_lookup(s);
				/* features are moved in index_lookup */
				assert(s->features == NULL);
			} else {
				NOTICE("Dedup phase: an empty segment");
				if (s->features)
					g_hash_table_destroy(s->features);
				s->features = NULL;
			}
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
	NOTICE("Dedup phase concludes: %d segments of %d chunks on average",
			segment_num, segment_num ? chunk_num / segment_num : 0);
}
