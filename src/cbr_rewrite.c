#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "storage/containerstore.h"
#include "backup.h"

static int64_t chunk_num;

static double get_rewrite_utility(struct chunk *c) {
	double rewrite_utility = 1;
	GSequenceIter *iter = g_sequence_lookup(rewrite_buffer.container_record_seq,
			&c->id, g_record_cmp_by_id, NULL);
	assert(iter);
	struct containerRecord *record = g_sequence_get(iter);
	double coverage = (record->size + c->size) / (double) (CONTAINER_SIZE - CONTAINER_META_SIZE);
	rewrite_utility = coverage >= 1 ? 0 : rewrite_utility - coverage;
	return rewrite_utility;
}

struct {
	int32_t chunk_num;
	double current_utility_threshold;
	int min_index;
	/* [0,1/10000), [1/10000, 2/10000), ... , [9999/10000, 1] */
	int32_t buckets[10000];
} utility_buckets;

/* init utility buckets */
void init_utility_buckets() {
	utility_buckets.chunk_num = 0;
	utility_buckets.min_index =	destor.rewrite_cbr_minimal_utility == 1 ?
					9999 : destor.rewrite_cbr_minimal_utility * 10000;
	utility_buckets.current_utility_threshold =
			destor.rewrite_cbr_minimal_utility;
	bzero(&utility_buckets.buckets, sizeof(utility_buckets.buckets));
}

static void utility_buckets_update(double rewrite_utility) {
	utility_buckets.chunk_num++;
	int index = rewrite_utility >= 1 ? 9999 : rewrite_utility * 10000;
	utility_buckets.buckets[index]++;
	if (utility_buckets.chunk_num >= 100) {
		int best_num = utility_buckets.chunk_num * destor.rewrite_cbr_limit;
		int current_index = 9999;
		int count = 0;
		for (; current_index >= utility_buckets.min_index; --current_index) {
			count += utility_buckets.buckets[current_index];
			if (count >= best_num) {
				break;
			}
		}
		utility_buckets.current_utility_threshold = (current_index + 1)
				/ 10000.0;
	}
}

/* --------------------------------------------------------------------------*/
/**
 * @Synopsis  Reducing impact of data fragmentation caused by in-line deduplication.
 *            In SYSTOR'12.
 *
 *			  We first buffer a fixed-sized buffer of chunks for the decision chunk.
 *			  Then, find all chunks in an identical container with the decision chunk.
 *			  If these chunks are large enough, they are all not fragmentation.
 *			  An important optimization is that,
 *			  if we find a decision chunk already being marked not fragmented,
 *			  we should ensure its physical neighbors in the buffer also being marked not fragmented.
 *			  This optimization is very important for CBR.
 * @Param
 *
 * @Returns   
 */
/* ----------------------------------------------------------------------------*/
void *cbr_rewrite(void* arg) {

	init_utility_buckets();

	/* content-based rewrite*/
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

		TIMER_BEGIN(1);
		struct chunk *decision_chunk = rewrite_buffer_top();
		while (CHECK_CHUNK(decision_chunk, CHUNK_FILE_START)
				|| CHECK_CHUNK(decision_chunk, CHUNK_FILE_END)
				|| CHECK_CHUNK(decision_chunk, CHUNK_SEGMENT_START)
				|| CHECK_CHUNK(decision_chunk, CHUNK_SEGMENT_END)) {
			rewrite_buffer_pop();
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_push(rewrite_queue, decision_chunk);
			TIMER_BEGIN(1);
			decision_chunk = rewrite_buffer_top();
		}

		TIMER_BEGIN(1);
		/* A normal chunk */
		double rewrite_utility = 0;

		if (decision_chunk->id != TEMPORARY_ID) {
			assert(CHECK_CHUNK(decision_chunk, CHUNK_DUPLICATE));
			/* a duplicate chunk */
			GSequenceIter *iter = g_sequence_lookup(
					rewrite_buffer.container_record_seq, &decision_chunk->id,
					g_record_cmp_by_id, NULL);
			assert(iter);
			struct containerRecord *record = g_sequence_get(iter);

			if (record->out_of_order == 1) {
				rewrite_utility = get_rewrite_utility(decision_chunk);
				if (rewrite_utility < destor.rewrite_cbr_minimal_utility
						|| rewrite_utility < utility_buckets.current_utility_threshold) {
					record->out_of_order = 0;
				} else {
					VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
							chunk_num, decision_chunk->id);
					SET_CHUNK(decision_chunk, CHUNK_OUT_OF_ORDER);
				}

			} else {
				/* if marked as not out of order*/
				rewrite_utility = 0;
			}
		}

		utility_buckets_update(rewrite_utility);
		chunk_num++;

		rewrite_buffer_pop();
		TIMER_END(1, jcr.rewrite_time);
		sync_queue_push(rewrite_queue, decision_chunk);
	}

	/* process the remaining chunks in stream context */
	struct chunk *remaining_chunk = NULL;
	while ((remaining_chunk = rewrite_buffer_pop()))
		sync_queue_push(rewrite_queue, remaining_chunk);
	sync_queue_term(rewrite_queue);

	return NULL;
}
