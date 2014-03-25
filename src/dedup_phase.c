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

/* defined in filter_phase.c */
extern struct {
	GMutex mutex;
	struct container *container_buffer;
} storage_buffer;

struct {
	/* g_mutex_init() is unnecessary if in static storage. */
	GMutex mutex;
	GCond not_full_cond; // index buffer is not full
	int wait_flag; // index buffer is full, waiting
} index_lock;

void send_segment(struct segment* s) {

	/*
	 * CHUNK_SEGMENT_START and _END are used for
	 * reconstructing the segment in filter phase.
	 */
	struct chunk* ss = new_chunk(0);
	SET_CHUNK(ss, CHUNK_SEGMENT_START);
	sync_queue_push(dedup_queue, ss);

	struct chunk* c;
	while ((c = g_queue_pop_head(s->chunks))) {
		if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)) {
			if (CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
				if (c->id == TEMPORARY_ID) {
					VERBOSE("Dedup phase: %ldth chunk is identical to a unique chunk",
							chunk_num++);
				} else {
					VERBOSE("Dedup phase: %ldth chunk is duplicate in container %lld",
							chunk_num++, c->id);
				}
			} else {
				VERBOSE("Dedup phase: %ldth chunk is unique", chunk_num++);
			}

		}
		sync_queue_push(dedup_queue, c);
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
			NOTICE("Dedup phase: the %lldth segment of %lld", segment_num++,
					s->chunk_num);
			/* Each duplicate chunk will be marked. */
			g_mutex_lock(&index_lock.mutex);
			while (index_lookup(s) == 0) {
				index_lock.wait_flag = 1;
				g_cond_wait(&index_lock.not_full_cond, &index_lock.mutex);
			}
			g_mutex_unlock(&index_lock.mutex);
		} else {
			NOTICE("Dedup phase: an empty segment");
		}
		/* Send chunks in the segment to the next phase.
		 * The segment will be cleared. */
		send_segment(s);

		free_segment(s, free_chunk);
		s = NULL;

		if (c == NULL)
			break;
	}

	sync_queue_term(dedup_queue);

	return NULL;
}

void start_dedup_phase() {
	dedup_queue = sync_queue_new(1000);

	pthread_create(&dedup_t, NULL, dedup_thread, NULL);
}

void stop_dedup_phase() {
	pthread_join(dedup_t, NULL);
	NOTICE("Dedup phase concludes: %d segments of %d chunks on average",
			segment_num, segment_num ? chunk_num / segment_num : 0);
}
