#include "global.h"
#include "dedup.h"
#include "index/index.h"
#include "jcr.h"
#include "tools/sync_queue.h"

extern SyncQueue *prepare_queue;
extern SyncQueue *container_queue;

extern int32_t capping_T;
extern int32_t capping_segment_size;
extern int rewriting_algorithm;
static GHashTable* historical_sparse_containers;
typedef struct{
    ContainerId cid;
    int32_t length;
}ContainerRecord;

struct{
    Queue *chunk_queue;
    GSequence *container_record_seq;//
    int32_t size;
}cap_segment;

static void cap_segment_init(){
    cap_segment.chunk_queue = queue_new();
    cap_segment.container_record_seq = g_sequence_new(free);
    cap_segment.size = 0;
}

static gint compare_cid(gconstpointer a, gconstpointer b, gpointer user_data){
    return ((ContainerRecord*)a)->cid - ((ContainerRecord*)b)->cid;
}

static gint compare_length(gconstpointer a, gconstpointer b, gpointer user_data){
    return ((ContainerRecord*)a)->length - ((ContainerRecord*)b)->length;
}

static BOOL cap_segment_push(Jcr *jcr, Chunk *chunk){
    if((cap_segment.size + chunk->length) > capping_segment_size){
        return FALSE;
    }
    chunk->container_id = index_search(&chunk->hash, &chunk->delegate);
    if(rewriting_algorithm == HBR_CAP_REWRITING && 
            chunk->container_id != TMP_CONTAINER_ID){
        if(historical_sparse_containers && 
                g_hash_table_contains(historical_sparse_containers,
                    &chunk->container_id)){
            chunk->container_id = TMP_CONTAINER_ID;
            jcr->rewritten_chunk_amount += chunk->length;
            jcr->rewritten_chunk_count++;
        }
    }
    if(chunk->container_id != TMP_CONTAINER_ID){
        ContainerRecord tmp_record;
        tmp_record.cid = chunk->container_id;
        GSequenceIter *iter = g_sequence_lookup(cap_segment.container_record_seq,
                &tmp_record, compare_cid, NULL);
        if(iter == NULL){
            ContainerRecord* record = malloc(sizeof(ContainerRecord));
            record->cid = chunk->container_id;
            record->length = chunk->length;
            g_sequence_insert_sorted(cap_segment.container_record_seq,
                    record, compare_cid, NULL);
        }else{
            ContainerRecord* record = g_sequence_get(iter);
            if(record->cid != chunk->container_id){
                dprint("error happens!");
            }
            record->length += chunk->length;
        }
    }
    queue_push(cap_segment.chunk_queue, chunk);
    cap_segment.size += chunk->length;
    return TRUE;
}

static Chunk* cap_segment_pop(){
    Chunk *chunk = queue_pop(cap_segment.chunk_queue);
    if(chunk == NULL){
        return NULL;
    }
    cap_segment.size -= chunk->length;
    return chunk;
}

static void cap_segment_clear(){
    g_sequence_remove_range(g_sequence_get_begin_iter(cap_segment.container_record_seq),
            g_sequence_get_end_iter(cap_segment.container_record_seq));
    if(cap_segment.size != 0){
        dprint("size != 0");
    }
    if(queue_size(cap_segment.chunk_queue) != 0){
        dprint("queue is not empty!");
    }
}

static void cap_segment_get_top(){
    int32_t length = g_sequence_get_length(cap_segment.container_record_seq);
    if(length <= capping_T)
        return;
    g_sequence_sort(cap_segment.container_record_seq,
            compare_length, NULL);
    /* remove extra records */
    int i = 0;
    for(; i<length - capping_T; ++i){
        g_sequence_remove(g_sequence_get_begin_iter(cap_segment.container_record_seq));
    }
    length = g_sequence_get_length(cap_segment.container_record_seq);
    if(length != capping_T)
        dprint("length != capping_T");

    g_sequence_sort(cap_segment.container_record_seq,
            compare_cid, NULL);
}

void *cap_filter(void* arg){
    Jcr *jcr = (Jcr*)arg;
    jcr->write_buffer = container_new_full();
    set_container_id(jcr->write_buffer);

    cap_segment_init();
    
    Chunk *chunk = 0, *remaining = 0;
    ContainerRecord tmp_record;
    BOOL stream_end = FALSE;

    historical_sparse_containers = load_historical_sparse_containers(jcr->job_id);
    ContainerUsageMonitor * monitor = container_usage_monitor_new();

    while(TRUE){
        chunk = sync_queue_pop(prepare_queue);
        TIMER_DECLARE(b1, e1);
        TIMER_BEGIN(b1);
        if(chunk->length == STREAM_END){
            free_chunk(chunk);
            chunk = 0;
            stream_end = TRUE;
        }
        if(stream_end == TRUE || cap_segment_push(jcr, chunk) == FALSE){
            /* segment is full */
            remaining = chunk;
            cap_segment_get_top();

            while(chunk = cap_segment_pop()){
                if(chunk->container_id != TMP_CONTAINER_ID){
                    tmp_record.cid = chunk->container_id;
                    if(g_sequence_lookup(cap_segment.container_record_seq,
                                &tmp_record, compare_cid, NULL)){
                        /* in TOP_T */
                        chunk->duplicate = TRUE;
                    }else{
                        chunk->duplicate = FALSE;
                        chunk->container_id = TMP_CONTAINER_ID;
                        jcr->rewritten_chunk_amount += chunk->length;
                        jcr->rewritten_chunk_count++;
                    }
                }else{
                    chunk->duplicate = FALSE;
                }

                if(chunk->duplicate == FALSE){
                    while(container_add_chunk(jcr->write_buffer, chunk)
                            == CONTAINER_FULL){
                        Container *container = jcr->write_buffer;
                        int32_t id = seal_container(container);
                        sync_queue_push(container_queue, container);
                        jcr->write_buffer = container_new_full();
                        set_container_id(jcr->write_buffer);
                    }
                    chunk->container_id = jcr->write_buffer->id;
                    index_insert(&chunk->hash, chunk->container_id,
                            &chunk->delegate);
                }else{
                    jcr->dedup_size += chunk->length;
                    ++jcr->number_of_dup_chunks;
                }

                FingerChunk *new_fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
                new_fchunk->container_id = chunk->container_id;
                new_fchunk->length = chunk->length;
                memcpy(&new_fchunk->fingerprint, &chunk->hash, sizeof(Fingerprint));
                container_usage_monitor_update(monitor, new_fchunk->container_id,
                        &new_fchunk->fingerprint, new_fchunk->length);
                sync_queue_push(jcr->fingerchunk_queue, new_fchunk);

                free_chunk(chunk);
            }//while

            cap_segment_clear();
            if(remaining)
                cap_segment_push(jcr, remaining);
        }//full or end
        if(stream_end == TRUE)
            break;
        TIMER_END(jcr->filter_time, b1, e1);
    }

    Container *container = jcr->write_buffer;
    jcr->write_buffer = 0;
    if(seal_container(container) != TMP_CONTAINER_ID){
        sync_queue_push(container_queue, container);
    }else{
        container_free_full(container);
    }

    Container *signal = container_new_meta_only();
    signal->id = STREAM_END;
    sync_queue_push(container_queue, signal);

    FingerChunk* fchunk_sig = (FingerChunk*)malloc(sizeof(FingerChunk));
    fchunk_sig->container_id = STREAM_END;
    sync_queue_push(jcr->fingerchunk_queue, fchunk_sig);

    queue_free(cap_segment.chunk_queue, free_chunk);
    g_sequence_free(cap_segment.container_record_seq);

    jcr->sparse_container_num = g_hash_table_size(monitor->sparse_map);
    jcr->total_container_num = g_hash_table_size(monitor->dense_map) + jcr->sparse_container_num;
    while((jcr->inherited_sparse_num = container_usage_monitor_print(monitor, 
                    jcr->job_id, historical_sparse_containers))<0){
        dprint("retry!");
    }
    return NULL;
}
