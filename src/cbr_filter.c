/**
 * @file cbr_filter.c
 * @Synopsis  
 * @author fumin, fumin@hust.edu.cn
 * @version 1
 * @date 2012-12-19
 */
#include "global.h"
#include "dedup.h"
#include "jcr.h"
#include "tools/sync_queue.h"
#include "storage/container_usage_monitor.h"

extern void send_fc_signal();
extern void send_fingerchunk(FingerChunk *fchunk, EigenValue* eigenvalue,
		BOOL update);

extern double container_usage_threshold;
extern double rewrite_limit;
extern int32_t stream_context_size;
extern int32_t disk_context_size;
extern double minimal_rewrite_utility;

extern int recv_chunk_with_eigenvalue(Chunk **chunk);
extern ContainerId save_chunk(Chunk *chunk);

typedef struct {
	Queue *context_queue;
	/* facilitate calculating rewrite utility */
	GHashTable *container_usage_map;
	int amount;
} StreamContext;

/* allocate memory for stream context */
static StreamContext* stream_context_new() {
	StreamContext* stream_context = (StreamContext*) malloc(
			sizeof(StreamContext));
	stream_context->context_queue = queue_new();
	stream_context->container_usage_map = g_hash_table_new_full(g_int_hash,
			g_int_equal, free, free);
	stream_context->amount = 0;
	return stream_context;
}

static void stream_context_free(StreamContext* stream_context) {
	g_hash_table_destroy(stream_context->container_usage_map);
	stream_context->container_usage_map = 0;
	queue_free(stream_context->context_queue, free_chunk);
	stream_context->context_queue = 0;

	free(stream_context);
}

static BOOL stream_context_push(StreamContext* stream_context, Chunk *chunk) {
	if ((stream_context->amount + chunk->length) > stream_context_size) {
		/* stream context is full */
		return FALSE;
	}
	if (chunk->status & DUPLICATE) {
		/* assuming every one is out of order*/
		chunk->status |= OUT_OF_ORDER;
		int* cntnr_usg = g_hash_table_lookup(
				stream_context->container_usage_map, &chunk->container_id);
		if (cntnr_usg == 0) {
			ContainerId *new_cntnr_id = malloc(sizeof(ContainerId));
			*new_cntnr_id = chunk->container_id;
			cntnr_usg = malloc(sizeof(int));
			*cntnr_usg = CONTAINER_DES_SIZE;
			g_hash_table_insert(stream_context->container_usage_map,
					new_cntnr_id, cntnr_usg);
		}
		*cntnr_usg += chunk->length + CONTAINER_META_ENTRY_SIZE;
	}

	stream_context->amount += chunk->length;
	queue_push(stream_context->context_queue, chunk);
	return TRUE;
}

static Chunk* stream_context_pop(StreamContext* stream_context) {
	Chunk *chunk = queue_pop(stream_context->context_queue);
	if (chunk == 0) {
		return NULL ;
	}
	stream_context->amount -= chunk->length;

	if (chunk->container_id != TMP_CONTAINER_ID) {
		int* cntnr_usg = g_hash_table_lookup(
				stream_context->container_usage_map, &chunk->container_id);
		*cntnr_usg -= chunk->length + CONTAINER_META_ENTRY_SIZE;
		if (*cntnr_usg == CONTAINER_DES_SIZE) {
			g_hash_table_remove(stream_context->container_usage_map,
					&chunk->container_id);
		}
	}

	return chunk;
}

static Chunk* stream_context_init(StreamContext* stream_context) {
	Chunk *chunk = NULL;
	int signal = recv_chunk_with_eigenvalue(&chunk);
	while (signal != STREAM_END) {
		if (stream_context_push(stream_context, chunk) == FALSE) {
			break;
		}
		signal = recv_chunk_with_eigenvalue(&chunk);
	}
	if (signal == STREAM_END) {
		free_chunk(chunk);
		return NULL ;
	}
	return chunk;
}

static double get_rewrite_utility(StreamContext *stream_context,
		ContainerId container_id) {
	double rewrite_utility = 1;
	int *cntnr_usg = g_hash_table_lookup(stream_context->container_usage_map,
			&container_id);
	if (cntnr_usg != 0) {
		double coverage = (*cntnr_usg)
				/ ((double) CONTAINER_SIZE * (disk_context_size));
		rewrite_utility = coverage >= 1 ? 0 : rewrite_utility - coverage;
	}
	return rewrite_utility;
}

static void mark_not_out_of_order(Chunk *chunk, ContainerId *container_id) {
	if (chunk->container_id == *container_id) {
		chunk->status &= ~OUT_OF_ORDER;
	}
}

typedef struct {
	int32_t chunk_count;
	double current_utility_threshold;
	int min_index;
	/* [0,1/10000), [1/10000, 2/10000), ... , [9999/10000, 1] */
	int32_t buckets[10000];
} UtilityBuckets;

/* init utility buckets */
static UtilityBuckets* utility_buckets_new() {
	UtilityBuckets* buckets = (UtilityBuckets*) malloc(sizeof(UtilityBuckets));
	buckets->chunk_count = 0;
	buckets->min_index =
			minimal_rewrite_utility == 1 ?
					9999 : minimal_rewrite_utility * 10000;
	buckets->current_utility_threshold = minimal_rewrite_utility;
	bzero(&buckets->buckets, sizeof(buckets->buckets));
	return buckets;
}

static void utility_buckets_update(UtilityBuckets *buckets,
		double rewrite_utility) {
	buckets->chunk_count++;
	int index = rewrite_utility >= 1 ? 9999 : rewrite_utility * 10000;
	buckets->buckets[index]++;
	if (buckets->chunk_count >= 100) {
		int best_num = buckets->chunk_count * rewrite_limit;
		int current_index = 9999;
		int count = 0;
		for (; current_index >= buckets->min_index; --current_index) {
			count += buckets->buckets[current_index];
			if (count >= best_num) {
				break;
			}
		}
		buckets->current_utility_threshold = (current_index + 1) / 10000.0;
	}
}

/* --------------------------------------------------------------------------*/
/**
 * @Synopsis  Reducing impact of data fragmentation caused by in-line deduplication.
 *            In SYSTOR'12.
 *
 * @Param arg
 *
 * @Returns   
 */
/* ----------------------------------------------------------------------------*/
void *cbr_filter(void* arg) {
	Jcr *jcr = (Jcr*) arg;

	StreamContext* stream_context = stream_context_new();
	Chunk *tail = stream_context_init(stream_context);

	UtilityBuckets *buckets = utility_buckets_new();

	int i = 0;
	/* content-based rewrite*/
	while (tail) {
		TIMER_DECLARE(b1, e1);
		TIMER_BEGIN(b1);
		Chunk *decision_chunk = stream_context_pop(stream_context);
		if (stream_context_push(stream_context, tail) == TRUE) {
			TIMER_END(jcr->filter_time, b1, e1);
			int signal = recv_chunk_with_eigenvalue(&tail);
			if (signal == STREAM_END) {
				free_chunk(tail);
				tail = NULL;
			}
		}
		TIMER_BEGIN(b1);
		double rewrite_utility = 0;
		/* if chunk has existed */
		if (decision_chunk->status & DUPLICATE) {
			/* a duplicate chunk */
			if (decision_chunk->status & OUT_OF_ORDER) {
				rewrite_utility = get_rewrite_utility(stream_context,
						decision_chunk->container_id);
				if (rewrite_utility < minimal_rewrite_utility
						|| rewrite_utility
								< buckets->current_utility_threshold) {
					/* mark all physically continuous chunks */
					decision_chunk->status &= ~OUT_OF_ORDER;
					queue_foreach(stream_context->context_queue,
							mark_not_out_of_order,
							&decision_chunk->container_id);
				}
			} else
				/* if marked as not out of order*/
				rewrite_utility = 0;
		}

		utility_buckets_update(buckets, rewrite_utility);

		BOOL update = FALSE;
		if (decision_chunk->status & DUPLICATE) {
			if ((decision_chunk->status & SPARSE)||
			(decision_chunk->status & NOT_IN_CACHE) &&
			(decision_chunk->status & OUT_OF_ORDER)){
			decision_chunk->container_id = save_chunk(decision_chunk);
			update = TRUE;
			jcr->rewritten_chunk_count ++;
			jcr->rewritten_chunk_amount += decision_chunk->length;
		} else {
			jcr->dedup_size += decision_chunk->length;
			++jcr->number_of_dup_chunks;
		}
		i++;
	} else {
		decision_chunk->container_id = save_chunk(decision_chunk);
		update = TRUE;
	}

		FingerChunk *new_fchunk = (FingerChunk*) malloc(sizeof(FingerChunk));
		new_fchunk->container_id = decision_chunk->container_id;
		new_fchunk->length = decision_chunk->length;
		memcpy(&new_fchunk->fingerprint, &decision_chunk->hash,
				sizeof(Fingerprint));
		TIMER_END(jcr->filter_time, b1, e1);
		send_fingerchunk(new_fchunk, decision_chunk->eigenvalue, update);
		free_chunk(decision_chunk);
	}

	TIMER_DECLARE(b1, e1);
	TIMER_BEGIN(b1);

	/* process the remaining chunks in stream context */
	Chunk *remaining_chunk = stream_context_pop(stream_context);
	while (remaining_chunk) {
		BOOL update = FALSE;
		if (remaining_chunk->status & DUPLICATE) {
			if (remaining_chunk->status & SPARSE) {
				remaining_chunk->container_id = save_chunk(remaining_chunk);
				update = TRUE;
				jcr->rewritten_chunk_count++;
				jcr->rewritten_chunk_amount += remaining_chunk->length;
			} else {
				jcr->dedup_size += remaining_chunk->length;
				++jcr->number_of_dup_chunks;
			}
			i++;
		} else {
			remaining_chunk->container_id = save_chunk(remaining_chunk);
			update = TRUE;
		}

		FingerChunk *new_fchunk = (FingerChunk*) malloc(sizeof(FingerChunk));
		new_fchunk->container_id = remaining_chunk->container_id;
		new_fchunk->length = remaining_chunk->length;
		memcpy(&new_fchunk->fingerprint, &remaining_chunk->hash,
				sizeof(Fingerprint));
		send_fingerchunk(new_fchunk, remaining_chunk->eigenvalue, update);

		free_chunk(remaining_chunk);
		remaining_chunk = stream_context_pop(stream_context);
	}
	TIMER_END(jcr->filter_time, b1, e1);
	printf("i=%d\n", i);

	save_chunk(NULL );

	send_fc_signal();

	free(buckets);
	stream_context_free(stream_context);

	return NULL ;
}
