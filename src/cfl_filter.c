/*
 * cfl_dedup.c
 *
 *  Created on: Sep 19, 2012
 *      Author: fumin
 */

#include "global.h"
#include "dedup.h"
#include "jcr.h"
#include "tools/sync_queue.h"
#include "storage/cfl_monitor.h"

extern void send_fc_signal();
extern void send_fingerchunk(FingerChunk *fchunk, 
        EigenValue* eigenvalue, BOOL update);
extern double cfl_require;
extern double cfl_usage_threshold;
extern CFLMonitor* cfl_monitor;

extern int recv_chunk_with_eigenvalue(Chunk **chunk);

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

static void rewrite_container(Jcr *jcr){
    Chunk *waiting_chunk = queue_pop(container_tmp.waiting_chunk_queue);
    while(waiting_chunk){
        FingerChunk* fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
        fchunk->container_id = waiting_chunk->container_id;
        fchunk->length = waiting_chunk->length;
        memcpy(&fchunk->fingerprint, &waiting_chunk->hash, sizeof(Fingerprint));
        fchunk->next = 0;
        if(waiting_chunk->data){
            fchunk->container_id = save_chunk(waiting_chunk);
            jcr->rewritten_chunk_count++;
            jcr->rewritten_chunk_amount += waiting_chunk->length;
        }
        send_fingerchunk(fchunk, waiting_chunk->eigenvalue, TRUE);
        free_chunk(waiting_chunk);
        waiting_chunk = queue_pop(container_tmp.waiting_chunk_queue);
    }
}

static void selective_dedup(Jcr *jcr, Chunk *new_chunk){
    BOOL update = FALSE;
    /* existed */
    if(container_tmp.id != new_chunk->container_id){
        if(container_tmp.size > 0){
            /* determining whether rewrite container_tmp */
            if((1.0*container_tmp.size/CONTAINER_SIZE) < cfl_usage_threshold 
                    && (container_tmp.status & NOT_IN_CACHE)
                    || container_tmp.status & SPARSE){
                /* 
                 * If SPARSE,  rewrite it.
                 * If OUT_OF_ORDER and NOT_IN_CACHE, rewrite it.
                 * */
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
                    send_fingerchunk(fchunk, waiting_chunk->eigenvalue, update);
                    free_chunk(waiting_chunk);
                    waiting_chunk = queue_pop(container_tmp.waiting_chunk_queue);
                }
            }
        }
        container_tmp.size = 0; 
        container_tmp.id = TMP_CONTAINER_ID;
    }
    if(new_chunk->status & DUPLICATE){
        /*
         * Set the status of container_tmp as the status of the first chunk
         */
        if(container_tmp.size == 0){
            container_tmp.size = CONTAINER_DES_SIZE;
            container_tmp.id = new_chunk->container_id;
        }
        container_tmp.size += new_chunk->length + CONTAINER_META_ENTRY_SIZE;
        container_tmp.status = new_chunk->status;

        queue_push(container_tmp.waiting_chunk_queue, new_chunk);
    }else{
        if(container_tmp.size > 0){
            dprint("container_tmp is not empty!");
        }
        save_chunk(new_chunk);

        FingerChunk* fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
        fchunk->container_id = new_chunk->container_id;
        fchunk->length = new_chunk->length;
        memcpy(&fchunk->fingerprint, &new_chunk->hash, sizeof(Fingerprint));
        fchunk->next = 0;
        send_fingerchunk(fchunk, new_chunk->eigenvalue, TRUE);
        free_chunk(new_chunk);
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
        if(new_chunk->status & SPARSE){
            jcr->rewritten_chunk_count++;
            jcr->rewritten_chunk_amount += fchunk->length;
            fchunk->container_id = save_chunk(new_chunk);
            update = TRUE;
        }else{
            jcr->dedup_size += fchunk->length;
            jcr->number_of_dup_chunks++;
        }
    }else{
        fchunk->container_id = save_chunk(new_chunk);
        update = TRUE;
    }
    send_fingerchunk(fchunk, new_chunk->eigenvalue, update);
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
    container_tmp.waiting_chunk_queue = queue_new();
    container_tmp.size = 0;
    container_tmp.id = TMP_CONTAINER_ID;

    double high_water_mark = cfl_require + 0.1;
    double low_water_mark = cfl_require;
    BOOL enable_selective = FALSE;

    while(TRUE){
        Chunk* new_chunk = NULL;
        int signal = recv_chunk_with_eigenvalue(&new_chunk);

        if (signal == STREAM_END) {
            free_chunk(new_chunk);
            break;
        }

        TIMER_DECLARE(b1, e1);
        TIMER_BEGIN(b1);
        if(enable_selective){
            /* selective deduplication */
            selective_dedup(jcr, new_chunk);

            if(get_cfl(cfl_monitor) > high_water_mark){
                enable_selective = FALSE;
                Chunk *chunk = queue_pop(container_tmp.waiting_chunk_queue);
                if(chunk){
                    /* It happens when the rewritten container improves CFL, */
                    FingerChunk* fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
                    fchunk->container_id = chunk->container_id;
                    fchunk->length = chunk->length;
                    memcpy(&fchunk->fingerprint, &chunk->hash, sizeof(Fingerprint));
                    fchunk->next = 0;

                    BOOL update = FALSE;
                    if(chunk->status & SPARSE){
                        fchunk->container_id = save_chunk(chunk);
                        jcr->rewritten_chunk_count++;
                        jcr->rewritten_chunk_amount += fchunk->length;
                        update = TRUE;
                    }else{
                        jcr->dedup_size += fchunk->length;
                        jcr->number_of_dup_chunks++;
                    }
                    send_fingerchunk(fchunk, chunk->eigenvalue, update);
                    free_chunk(chunk);    
                }
                if(queue_size(container_tmp.waiting_chunk_queue)!=0){
                    printf("%s, %d: irregular state in queue. size=%d.\n",
                            __FILE__,__LINE__, queue_size(container_tmp.waiting_chunk_queue));
                }
                container_tmp.size = 0;
                container_tmp.id = TMP_CONTAINER_ID;
            }   
        }else{
            /* typical dedup */
            typical_dedup(jcr, new_chunk);

            if(get_cfl(cfl_monitor) < low_water_mark){
                enable_selective = TRUE;
            }
        }
        TIMER_END(jcr->filter_time, b1, e1);
    }

    /* Handle container_tmp*/
    if(enable_selective){
        if((1.0*container_tmp.size/CONTAINER_SIZE) < cfl_usage_threshold 
                && (container_tmp.status & NOT_IN_CACHE)
                || container_tmp.status & SPARSE){
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
                send_fingerchunk(fchunk, waiting_chunk->eigenvalue, update);
                free_chunk(waiting_chunk);
                waiting_chunk = queue_pop(container_tmp.waiting_chunk_queue);
            }
        }
    }

    send_fc_signal();

    save_chunk(NULL);

    queue_free(container_tmp.waiting_chunk_queue, 0);
    return NULL;
}
