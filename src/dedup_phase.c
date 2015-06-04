/*
 * In the phase,
 * we aggregate chunks into segments,
 * and deduplicate each segment with its similar segments.
 * Duplicate chunks are identified and marked.
 * For fingerprint indexes exploiting physical locality (e.g., DDFS, Sampled Index),
 * segments are only for batch process.
 * */
#include "destor.h"
#include "jcr.h"
#include "index/index.h"
#include "backup.h"
#include "storage/containerstore.h"

static pthread_t dedup_t;
static int64_t chunk_num;
static int64_t segment_num;

struct {
	/* g_mutex_init() is unnecessary if in static storage. */
	pthread_mutex_t mutex;
	pthread_cond_t cond; // index buffer is not full
	// index buffer is full, waiting
	// if threshold < 0, it indicates no threshold.
	int wait_threshold;
} index_lock;

void send_segment(struct segment* s) {
	/*
	 * CHUNK_SEGMENT_START and _END are used for
	 * reconstructing the segment in filter phase.
	 */
	struct chunk* ss = new_chunk(0);
	SET_CHUNK(ss, CHUNK_SEGMENT_START);
	sync_queue_push(dedup_queue, ss);

	GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
	GSequenceIter *begin = g_sequence_get_begin_iter(s->chunks);
	while(begin != end) {
		struct chunk* c = g_sequence_get(begin);
		if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)) {
			if (CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
				if (c->id == TEMPORARY_ID) {
					DEBUG("Dedup phase: %ldth chunk is identical to a unique chunk",
							chunk_num++);
				} else {
					DEBUG("Dedup phase: %ldth chunk is duplicate in container %lld",
							chunk_num++, c->id);
				}
			} else {
				DEBUG("Dedup phase: %ldth chunk is unique", chunk_num++);
			}

		}
		sync_queue_push(dedup_queue, c);
		g_sequence_remove(begin);
		begin = g_sequence_get_begin_iter(s->chunks);
	}

	struct chunk* se = new_chunk(0);
	SET_CHUNK(se, CHUNK_SEGMENT_END);
	sync_queue_push(dedup_queue, se);

	s->chunk_num = 0;

}

void *dedup_thread(void *arg) {
	struct segment* s = NULL;
	while (1) {
		struct chunk *c = NULL;
		if (destor.simulation_level != SIMULATION_ALL)
			c = sync_queue_pop(hash_queue);
		else
			c = sync_queue_pop(trace_queue);

		/* Add the chunk to the segment. */
		s = segmenting(c);
		if (!s)
			continue;
		/* segmenting success */
		if (s->chunk_num > 0) {
			VERBOSE("Dedup phase: the %lldth segment of %lld chunks", segment_num++,
					s->chunk_num);
			/* Each duplicate chunk will be marked. */
			pthread_mutex_lock(&index_lock.mutex);
			while (index_lookup(s) == 0) {
				pthread_cond_wait(&index_lock.cond, &index_lock.mutex);
			}
			pthread_mutex_unlock(&index_lock.mutex);
		} else {
			VERBOSE("Dedup phase: an empty segment");
		}
		/* Send chunks in the segment to the next phase.
		 * The segment will be cleared. */
		send_segment(s);

		free_segment(s);
		s = NULL;

		if (c == NULL)
			break;
	}

	sync_queue_term(dedup_queue);

	return NULL;
}

void start_dedup_phase() {

	if(destor.index_segment_algorithm[0] == INDEX_SEGMENT_CONTENT_DEFINED)
		index_lock.wait_threshold = destor.rewrite_algorithm[1] + destor.index_segment_max - 1;
	else if(destor.index_segment_algorithm[0] == INDEX_SEGMENT_FIXED)
		index_lock.wait_threshold = destor.rewrite_algorithm[1] + destor.index_segment_algorithm[1] - 1;
	else
		index_lock.wait_threshold = -1; // file-defined segmenting has no threshold.

	pthread_mutex_init(&index_lock.mutex, NULL);
	pthread_cond_init(&index_lock.cond, NULL);

	dedup_queue = sync_queue_new(1000);

	pthread_create(&dedup_t, NULL, dedup_thread, NULL);
}

void stop_dedup_phase() {
	pthread_join(dedup_t, NULL);
	NOTICE("dedup phase stops successfully: %d segments of %d chunks on average",
			segment_num, segment_num ? chunk_num / segment_num : 0);
}
