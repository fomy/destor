/*
 * cfl_dedup.c
 *
 *  Created on: Sep 19, 2012
 *      Author: fumin
 */

#include "global.h"
#include "dedup.h"
#include "index/index.h"
#include "jcr.h"
#include "tools/sync_queue.h"
#include "storage/cfl_monitor.h"

extern void send_fc_signal();
extern void send_fingerchunk(FingerChunk *fchunk, 
        Fingerprint *feature, BOOL update);

extern int read_cache_size;
extern double cfl_require;

extern double cfl_usage_threshold;

extern int recv_feature(Chunk **chunk);

extern ContainerId save_chunk(Chunk *chunk);

/*static Container* container_tmp;*/
struct {
    /*
     * The chunks with undetermined container id will be pushed into this queue.
     * If container_tmp is empty, this queue is empty too.
     * */
    Queue *waiting_chunk_queue;
    int size;
    int status;
    ContainerId id;
} container_tmp;

/* Monitoring cfl in storage system. */
static CFLMonitor* monitor;

static void rewrite_container(Jcr *jcr){
    Chunk *waiting_chunk = queue_pop(container_tmp.waiting_chunk_queue);
    while(waiting_chunk){
        FingerChunk* fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
        fchunk->container_id = waiting_chunk->container_id;
        fchunk->length = waiting_chunk->length;
        memcpy(&fchunk->fingerprint, &waiting_chunk->hash, sizeof(Fingerprint));
        fchunk->next = 0;
        if(waiting_chunk->data){
            memcpy(&waiting_chunk->feature, &waiting_chunk->feature, sizeof(Fingerprint));
            fchunk->container_id = save_chunk(waiting_chunk);
            jcr->rewritten_chunk_count++;
            jcr->rewritten_chunk_amount += waiting_chunk->length;
        }
        update_cfl(monitor, fchunk->container_id, fchunk->length);
        send_fingerchunk(fchunk, &waiting_chunk->feature, TRUE);
        free_chunk(waiting_chunk);
        waiting_chunk = queue_pop(container_tmp.waiting_chunk_queue);
    }
}

static void selective_dedup(Jcr *jcr, Chunk *new_chunk){
    BOOL update = FALSE;
    if(new_chunk->status & DUPLICATE){
        /* existed */
        if(container_tmp.size != 0
                && container_tmp.id != new_chunk->container_id){
            /* determining whether rewrite container_tmp */
            if((1.0*container_tmp.size/CONTAINER_SIZE) < cfl_usage_threshold
                    || container_tmp.status & SPARSE){
                /* rewrite */
                rewrite_container(jcr);
            }else{
                Chunk* waiting_chunk = queue_pop(container_tmp.waiting_chunk_queue);
                while(waiting_chunk){
                    FingerChunk* fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
                    fchunk->container_id = waiting_chunk->container_id;
                    fchunk->length = waiting_chunk->length;
                    memcpy(&fchunk->fingerprint, &waiting_chunk->hash, sizeof(Fingerprint));
                    fchunk->next = 0;
                    if(waiting_chunk->data){
                        jcr->dedup_size += fchunk->length;
                        jcr->number_of_dup_chunks++;
                        update = FALSE;
                    }else{
                        update = TRUE;
                    }
                    update_cfl(monitor, fchunk->container_id, fchunk->length);
                    send_fingerchunk(fchunk, &waiting_chunk->feature, update);
                    free_chunk(waiting_chunk);
                    waiting_chunk = queue_pop(container_tmp.waiting_chunk_queue);
                }
            }
            container_tmp.size = CONTAINER_DES_SIZE;
        }
        container_tmp.id = new_chunk->container_id;
        container_tmp.size += new_chunk->length + CONTAINER_META_ENTRY_SIZE;
        container_tmp.status = new_chunk->status;
        queue_push(container_tmp.waiting_chunk_queue, new_chunk);
    } else {
        save_chunk(new_chunk);

        if(queue_size(container_tmp.waiting_chunk_queue) == 0){
            FingerChunk* fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
            fchunk->container_id = new_chunk->container_id;
            fchunk->length = new_chunk->length;
            memcpy(&fchunk->fingerprint, &new_chunk->hash, sizeof(Fingerprint));
            fchunk->next = 0;
            free_chunk(new_chunk);
            update_cfl(monitor, fchunk->container_id, fchunk->length);
            send_fingerchunk(fchunk, &new_chunk->feature, TRUE);
        }else{
            free(new_chunk->data);
            new_chunk->data = 0;
            queue_push(container_tmp.waiting_chunk_queue, new_chunk);
        }
    }
}

static void typical_dedup(Jcr *jcr, Chunk *new_chunk){
    FingerChunk* fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
    fchunk->container_id = TMP_CONTAINER_ID;// tmp value
    fchunk->length = new_chunk->length;
    memcpy(&fchunk->fingerprint, &new_chunk->hash, sizeof(Fingerprint));
    fchunk->next = 0;

    BOOL update = FALSE;
    fchunk->container_id = new_chunk->container_id;
    if(new_chunk->status & DUPLICATE){
        jcr->dedup_size += fchunk->length;
        jcr->number_of_dup_chunks++;
    }else{
        fchunk->container_id = save_chunk(new_chunk);
        update = TRUE;
    }
    update_cfl(monitor, fchunk->container_id, fchunk->length);
    send_fingerchunk(fchunk, &new_chunk->feature, update);
    free_chunk(new_chunk);
}

/*
 * Intuitively, cfl_filter() would not works well with ddfs_index.
 * Due of the locality preserved caching in ddfs_index, 
 * some unfortunate chunks in previous containers may be rewritten repeatedly.
 */
/* --------------------------------------------------------------------------*/
/**
 * @Synopsis  Assuring Demanded Read Performance of Data Deduplication Storage
 *            with Backup Datasets. In MASCOTS'12.
 *
 * @Param arg
 *
 * @Returns   
 */
/* ----------------------------------------------------------------------------*/
void *cfl_filter(void* arg){
    Jcr* jcr = (Jcr*) arg;
    monitor = cfl_monitor_new(read_cache_size, cfl_require);
    container_tmp.waiting_chunk_queue = queue_new();
    container_tmp.size = 0;
    container_tmp.id = TMP_CONTAINER_ID;
    while(TRUE){
        Chunk* new_chunk = NULL;
        int signal = recv_feature(&new_chunk);

        if (signal == STREAM_END) {
            free_chunk(new_chunk);
            break;
        }

        TIMER_DECLARE(b1, e1);
        TIMER_BEGIN(b1);
        if(monitor->enable_selective){
            /* selective deduplication */
            selective_dedup(jcr, new_chunk);

            if(get_cfl(monitor) > monitor->high_water_mark){
                monitor->enable_selective = FALSE;
                Chunk *chunk = queue_pop(container_tmp.waiting_chunk_queue);
                if(chunk){
                    /* It happens when the rewritten container improves CFL, */
                    FingerChunk* fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
                    fchunk->container_id = chunk->container_id;
                    fchunk->length = chunk->length;
                    memcpy(&fchunk->fingerprint, &chunk->hash, sizeof(Fingerprint));
                    fchunk->next = 0;
                    free_chunk(chunk);    
                    jcr->dedup_size += fchunk->length;
                    jcr->number_of_dup_chunks++;
                    update_cfl(monitor, fchunk->container_id, fchunk->length);
                    send_fingerchunk(fchunk, &chunk->feature, FALSE);
                }
                if(queue_size(container_tmp.waiting_chunk_queue)!=0){
                    printf("%s, %d: irregular state in queue. size=%d.\n",
                            __FILE__,__LINE__, queue_size(container_tmp.waiting_chunk_queue));
                }
                container_tmp.size = 0;
            }   
        }else{
            /* typical dedup */
            typical_dedup(jcr, new_chunk);

            if(get_cfl(monitor) < monitor->low_water_mark){
                monitor->enable_selective = TRUE;
            }
        }
        TIMER_END(jcr->filter_time, b1, e1);
    }

    /* Handle container_tmp*/
    if(monitor->enable_selective){
        if ((1.0*container_tmp.size/CONTAINER_SIZE)< cfl_usage_threshold) {
            //rewrite container_tmp
            rewrite_container(jcr);
        } else {
            BOOL update = FALSE;
            Chunk* waiting_chunk = queue_pop(container_tmp.waiting_chunk_queue);
            while(waiting_chunk){
                FingerChunk* fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
                fchunk->container_id = waiting_chunk->container_id;
                fchunk->length = waiting_chunk->length;
                memcpy(&fchunk->fingerprint, &waiting_chunk->hash, sizeof(Fingerprint));
                fchunk->next = 0;
                if(waiting_chunk->data){
                    jcr->dedup_size += fchunk->length;
                    jcr->number_of_dup_chunks++;
                    /*fchunk->container_id = container_tmp->id;*/
                    update = FALSE;
                }else{
                    update = TRUE;
                }
                update_cfl(monitor, fchunk->container_id, fchunk->length);
                send_fingerchunk(fchunk, &waiting_chunk->feature, update);
                free_chunk(waiting_chunk);
                waiting_chunk = queue_pop(container_tmp.waiting_chunk_queue);
            }
        }
    }

    send_fc_signal();

    save_chunk(NULL);

    print_cfl(monitor);
    cfl_monitor_free(monitor);
    queue_free(container_tmp.waiting_chunk_queue, 0);
    return NULL;
}
