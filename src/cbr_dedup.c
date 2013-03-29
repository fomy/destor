/**
 * @file cbr_filter.c
 * @Synopsis  
 * @author fumin, fumin@hust.edu.cn
 * @version 1
 * @date 2012-12-19
 */
#include "global.h"
#include "dedup.h"
#include "index/index.h"
#include "jcr.h"
#include "tools/sync_queue.h"
#include "storage/container_usage_monitor.h"
#include "tools/bloom_filter.h"

extern double container_usage_threshold;
extern double rewrite_limit;
extern int32_t stream_context_size;
extern int32_t disk_context_size;
extern int32_t rewriting_algorithm;
extern int32_t bloom_filter_size;
extern double minimal_rewrite_utility;

/* input */
/* hash queue */
extern SyncQueue* prepare_queue;

/* output */
/* container queue */
extern SyncQueue* container_queue;

static unsigned char *bloom_filter;

typedef struct{
    Queue *context_queue;
    /* facilitate calculating rewrite utility */
    GHashTable *container_usage_map;
    int amount;
}StreamContext;

/* allocate memory for stream context */
static StreamContext* stream_context_new(){
    StreamContext* stream_context = (StreamContext*)malloc(sizeof(StreamContext));
    stream_context->context_queue = queue_new();
    stream_context->container_usage_map = g_hash_table_new_full(g_int_hash,
            g_int_equal, free, free);
    stream_context->amount = 0;
    return stream_context;
}

static void stream_context_free(StreamContext* stream_context){
    g_hash_table_destroy(stream_context->container_usage_map);
    stream_context->container_usage_map = 0;
    queue_free(stream_context->context_queue, free_chunk);
    stream_context->context_queue = 0;

    free(stream_context);
}

static BOOL stream_context_push(StreamContext* stream_context, Chunk *chunk){
    if((stream_context->amount + chunk->length) > stream_context_size){
        /* stream context is full */
        return FALSE;
    }
    chunk->container_id = index_search(&chunk->hash, &chunk->feature);
    if(chunk->container_id != TMP_CONTAINER_ID){
        int* cntnr_usg = g_hash_table_lookup(stream_context->container_usage_map, &chunk->container_id);
        if(cntnr_usg == 0){
            ContainerId *new_cntnr_id = malloc(sizeof(ContainerId));
            *new_cntnr_id = chunk->container_id;
            cntnr_usg = malloc(sizeof(int));
            *cntnr_usg = CONTAINER_DES_SIZE;
            g_hash_table_insert(stream_context->container_usage_map, new_cntnr_id, cntnr_usg);
        }
        *cntnr_usg += chunk->length + CONTAINER_META_ENTRY_SIZE;
    }

    stream_context->amount += chunk->length;
    queue_push(stream_context->context_queue, chunk);
    return TRUE;
}

static Chunk* stream_context_pop(StreamContext* stream_context){
    Chunk *chunk = queue_pop(stream_context->context_queue);
    if(chunk == 0){
        return NULL;
    }
    stream_context->amount -= chunk->length;

    if(chunk->container_id != TMP_CONTAINER_ID){
        int* cntnr_usg = g_hash_table_lookup(stream_context->container_usage_map, &chunk->container_id);
        *cntnr_usg -= chunk->length + CONTAINER_META_ENTRY_SIZE;
        if(*cntnr_usg == CONTAINER_DES_SIZE){
            g_hash_table_remove(stream_context->container_usage_map, &chunk->container_id);
        }
    }

    return chunk;
}

static Chunk* stream_context_init(StreamContext* stream_context){
    Chunk *chunk = sync_queue_pop(prepare_queue);
    while(chunk->length != STREAM_END){
        if(stream_context_push(stream_context, chunk)==FALSE){
            break;
        }
        chunk = sync_queue_pop(prepare_queue);
    }
    /*free_chunk(chunk);*/
    /* small job, stream_context is not full */
    return chunk;
}

static double get_rewrite_utility(StreamContext *stream_context, ContainerId container_id){
    double rewrite_utility = 1;
    int *cntnr_usg = g_hash_table_lookup(stream_context->container_usage_map, &container_id);
    if(cntnr_usg != 0){
        double coverage = (*cntnr_usg)/((double)CONTAINER_SIZE*(disk_context_size));
        rewrite_utility = coverage>=1 ? 0 : rewrite_utility - coverage;
    }
    return rewrite_utility;
}

static void mark_duplicate(Chunk *chunk, ContainerId *container_id){
    if(chunk->container_id == *container_id){
        chunk->duplicate = TRUE;
    }
}

typedef struct{
    int32_t chunk_count;
    double current_utility_threshold;
    int min_index;
    /* [0,1/10000), [1/10000, 2/10000), ... , [9999/10000, 1] */
    int32_t buckets[10000];
}UtilityBuckets;

/* init utility buckets */
static UtilityBuckets* utility_buckets_new(){
    UtilityBuckets* buckets = (UtilityBuckets*)malloc(sizeof(UtilityBuckets));
    buckets->chunk_count = 0;
    buckets->min_index = minimal_rewrite_utility== 1 ? 9999 : minimal_rewrite_utility*10000;
    buckets->current_utility_threshold = minimal_rewrite_utility;
    bzero(&buckets->buckets, sizeof(buckets->buckets));
    return buckets;
}

static void utility_buckets_update(UtilityBuckets *buckets, double rewrite_utility){
    buckets->chunk_count++;
    int index = rewrite_utility >= 1 ? 9999 : rewrite_utility*10000;
    buckets->buckets[index]++;
    if(buckets->chunk_count >= 100){
        int best_num = buckets->chunk_count*rewrite_limit;
        int current_index = 9999;
        int count = 0;
        for(; current_index >= buckets->min_index; --current_index){
            count += buckets->buckets[current_index];
            if(count >= best_num){
                break;
            }
        }
        buckets->current_utility_threshold = (current_index+1)/10000.0;
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
void *cbr_filter(void* arg){
    Jcr *jcr = (Jcr*)arg;
    jcr->write_buffer = create_container();

    /*BOOL exist = FALSE;*/

    StreamContext* stream_context = stream_context_new();
    Chunk *tail = stream_context_init(stream_context);

    UtilityBuckets *buckets = utility_buckets_new();

    GHashTable* historical_sparse_containers = 0;
    historical_sparse_containers = load_historical_sparse_containers(jcr->job_id);

    ContainerUsageMonitor* monitor =container_usage_monitor_new();
    /* content-based rewrite*/
    while(tail->length != STREAM_END){
        TIMER_DECLARE(b1, e1);
        TIMER_BEGIN(b1);
        Chunk *decision_chunk = stream_context_pop(stream_context);
        if(stream_context_push(stream_context, tail) == TRUE){
            TIMER_END(jcr->filter_time, b1, e1);
            tail = sync_queue_pop(prepare_queue);
        }
        TIMER_BEGIN(b1);
        double rewrite_utility = -1;
        /* if chunk has existed */
        if(decision_chunk->container_id != TMP_CONTAINER_ID){
            /* a duplicate chunk */
            if(decision_chunk->duplicate == FALSE)
                rewrite_utility = get_rewrite_utility(stream_context, decision_chunk->container_id);
            else /* if marked as duplicate */
                rewrite_utility = 0;
            if(rewriting_algorithm == HBR_CBR_REWRITING){
                /* rewriting_algorithm == HBR_CBR */
                if(historical_sparse_containers && 
                        g_hash_table_lookup(historical_sparse_containers,
                            &decision_chunk->container_id) != NULL){
                    /* in sparse containers */
                    decision_chunk->duplicate = FALSE;
                    decision_chunk->container_id = TMP_CONTAINER_ID;
                    jcr->rewritten_chunk_count ++;
                    jcr->rewritten_chunk_amount += decision_chunk->length;
                }
            }
        }

        /* if chunk has existed, but have not been marked yet. */
        if(decision_chunk->container_id != TMP_CONTAINER_ID){
            /* not unique chunk or not be filter by HBR */
            if(decision_chunk->duplicate == FALSE){
                /* not mark as duplicate */
                if(rewrite_utility >= minimal_rewrite_utility
                        && rewrite_utility >= buckets->current_utility_threshold){
                    /* it's a condidate */
                    decision_chunk->duplicate = FALSE;
                    decision_chunk->container_id = TMP_CONTAINER_ID;
                    jcr->rewritten_chunk_count ++;
                    jcr->rewritten_chunk_amount += decision_chunk->length;
                }else{//<minimal or current
                    /* mark all physically continuous chunks duplicated */
                    decision_chunk->duplicate = TRUE;
                    queue_foreach(stream_context->context_queue, 
                            mark_duplicate, &decision_chunk->container_id);
                }
            }
        }

        if(rewrite_utility != -1){
            /* duplicate chunk */
            utility_buckets_update(buckets, rewrite_utility);
        }else{
            utility_buckets_update(buckets, 0);
        }

        BOOL update = FALSE;
        if(decision_chunk->duplicate == FALSE){
            while (container_add_chunk(jcr->write_buffer, decision_chunk)
                    == CONTAINER_FULL) {
                Container *container = jcr->write_buffer;
                seal_container(container);
                /*sync_queue_push(container_queue, container);*/
                jcr->write_buffer = create_container();
            }
            decision_chunk->container_id = jcr->write_buffer->id;
            update = TRUE;
        }else{
            jcr->dedup_size += decision_chunk->length;
            ++jcr->number_of_dup_chunks;
        }

        FingerChunk *new_fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
        new_fchunk->container_id = decision_chunk->container_id;
        new_fchunk->length = decision_chunk->length;
        memcpy(&new_fchunk->fingerprint, &decision_chunk->hash, sizeof(Fingerprint));
        TIMER_END(jcr->filter_time, b1, e1);
        container_usage_monitor_update(monitor, new_fchunk->container_id,
                &new_fchunk->fingerprint, new_fchunk->length);
            index_insert(&decision_chunk->hash, decision_chunk->container_id,
                    &decision_chunk->feature, update);
        sync_queue_push(jcr->fingerchunk_queue, new_fchunk);
        free_chunk(decision_chunk);
    }

    free_chunk(tail);
    TIMER_DECLARE(b1, e1);
    TIMER_BEGIN(b1);
    /* process the remaining chunks in stream context */
    Chunk *remaining_chunk = stream_context_pop(stream_context);
    while(remaining_chunk){
        BOOL update = FALSE;
        if(rewriting_algorithm == HBR_CBR_REWRITING &&
                remaining_chunk->container_id != TMP_CONTAINER_ID){ 
            if(historical_sparse_containers &&
                    g_hash_table_lookup(historical_sparse_containers, 
                        &remaining_chunk->container_id) != NULL){
                /* in sparse containers */
                remaining_chunk->duplicate = FALSE;
                remaining_chunk->container_id = TMP_CONTAINER_ID;
                jcr->rewritten_chunk_count ++;
                jcr->rewritten_chunk_amount += remaining_chunk->length;
            }
        }
        if(remaining_chunk->container_id == TMP_CONTAINER_ID){
            while (container_add_chunk(jcr->write_buffer, remaining_chunk)
                    == CONTAINER_FULL) {
                Container *container = jcr->write_buffer;
                seal_container(container);
                /*sync_queue_push(container_queue, container);*/
                jcr->write_buffer = create_container();
            }    
            remaining_chunk->container_id = jcr->write_buffer->id;
            update = TRUE;
        }else{
            jcr->dedup_size += remaining_chunk->length;
            ++jcr->number_of_dup_chunks;
        }
        FingerChunk *new_fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
        new_fchunk->container_id = remaining_chunk->container_id;
        new_fchunk->length = remaining_chunk->length;
        memcpy(&new_fchunk->fingerprint, &remaining_chunk->hash, sizeof(Fingerprint));
        container_usage_monitor_update(monitor, new_fchunk->container_id,
                &new_fchunk->fingerprint, new_fchunk->length);
            index_insert(&remaining_chunk->hash, remaining_chunk->container_id,
                    &remaining_chunk->feature, update);
        sync_queue_push(jcr->fingerchunk_queue, new_fchunk);
        free_chunk(remaining_chunk);
        remaining_chunk = stream_context_pop(stream_context);
    }
    TIMER_END(jcr->filter_time, b1, e1);

    Container *container = jcr->write_buffer;
    jcr->write_buffer = 0;
    seal_container(container);

    Container *signal = container_new_meta_only();
    signal->id = STREAM_END;
    sync_queue_push(container_queue, signal);

    FingerChunk* fchunk_sig = (FingerChunk*)malloc(sizeof(FingerChunk));
    fchunk_sig->container_id = STREAM_END;
    sync_queue_push(jcr->fingerchunk_queue, fchunk_sig);

    free(buckets);
    stream_context_free(stream_context);

    jcr->sparse_container_num = g_hash_table_size(monitor->sparse_map);
    jcr->total_container_num = g_hash_table_size(monitor->dense_map) + jcr->sparse_container_num;
    while((jcr->inherited_sparse_num = container_usage_monitor_print(monitor, 
                    jcr->job_id, historical_sparse_containers))<0){
        dprint("retry!");
    }
    if(historical_sparse_containers)
        destroy_historical_sparse_containers(historical_sparse_containers);
    container_usage_monitor_free(monitor);
    return NULL;
}
