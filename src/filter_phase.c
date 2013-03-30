#include "global.h"
#include "dedup.h" 
#include "jcr.h"
#include "tools/sync_queue.h"
#include "index/index.h"
#include "storage/protos.h"

extern int rewriting_algorithm;

/* output of prepare_thread */
extern SyncQueue* prepare_queue;

/* container queue */
extern SyncQueue* container_queue;

void free_chunk(Chunk* chunk){
    if(chunk->data && chunk->length>0)
        free(chunk->data);
    chunk->data = 0;
    free(chunk);
}

void* simply_filter(void* arg){
    Jcr* jcr = (Jcr*) arg;
    jcr->write_buffer = create_container();
    GHashTable* historical_sparse_containers = 0;
    historical_sparse_containers = load_historical_sparse_containers(jcr->job_id);
    ContainerUsageMonitor* monitor =container_usage_monitor_new();
    while (TRUE) {
        BOOL update = FALSE;
        Chunk* chunk = sync_queue_pop(prepare_queue);

        TIMER_DECLARE(b1, e1);
        TIMER_BEGIN(b1);
        if (chunk->length == STREAM_END) {
            free_chunk(chunk);
            break;
        }
        /* init FingerChunk */
        FingerChunk *new_fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
        new_fchunk->container_id = TMP_CONTAINER_ID;
        new_fchunk->length = chunk->length;
        memcpy(&new_fchunk->fingerprint, &chunk->hash, sizeof(Fingerprint));

        /*new_fchunk->container_id = index_search(&chunk->hash, &chunk->feature);*/
        if (new_fchunk->container_id != TMP_CONTAINER_ID) {
            if(rewriting_algorithm == HBR_REWRITING && historical_sparse_containers!=0 && 
                    g_hash_table_lookup(historical_sparse_containers, &new_fchunk->container_id) != NULL){
                /* this chunk is in a sparse container */
                /* rewrite it */
                chunk->duplicate = FALSE;
                while (container_add_chunk(jcr->write_buffer, chunk)
                        == CONTAINER_FULL) {
                    Container *container = jcr->write_buffer;
                    int32_t id = seal_container(container);
                    /*sync_queue_push(container_queue, container);*/
                    jcr->write_buffer = create_container();
                }
                new_fchunk->container_id = jcr->write_buffer->id; 
                update = TRUE;
                jcr->rewritten_chunk_count++;
                jcr->rewritten_chunk_amount += new_fchunk->length;
            }else{
                chunk->duplicate = TRUE;
                jcr->dedup_size += chunk->length;
                ++jcr->number_of_dup_chunks;
            }
        } else {
            chunk->duplicate = FALSE;
            while (container_add_chunk(jcr->write_buffer, chunk)
                    == CONTAINER_FULL) {
                Container *container = jcr->write_buffer;
                int32_t id = seal_container(container);
                /*sync_queue_push(container_queue, container);*/
                jcr->write_buffer = create_container();
            }
            new_fchunk->container_id = jcr->write_buffer->id; 
            update = TRUE;
        }
        container_usage_monitor_update(monitor, new_fchunk->container_id,
                &new_fchunk->fingerprint, new_fchunk->length);
        index_insert(&new_fchunk->fingerprint, new_fchunk->container_id, &chunk->feature, update);
        sync_queue_push(jcr->fingerchunk_queue, new_fchunk);
        TIMER_END(jcr->filter_time, b1, e1);
        free_chunk(chunk);
    }//while(TRUE) end

    Container *container = jcr->write_buffer;
    jcr->write_buffer = 0;
    seal_container(container);

    /* kill the append_thread */
    Container *signal = container_new_meta_only();
    signal->id = STREAM_END;
    sync_queue_push(container_queue, signal);

    FingerChunk* fchunk_sig = (FingerChunk*)malloc(sizeof(FingerChunk));
    fchunk_sig->container_id = STREAM_END;
    sync_queue_push(jcr->fingerchunk_queue, fchunk_sig);


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
