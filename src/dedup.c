/*
 * dedup.c
 *
 *  Created on: Sep 21, 2012
 *      Author: fumin
 */

#include "global.h"
#include "dedup.h" 
#include "jcr.h"
#include "tools/sync_queue.h"
#include "tools/chunking.h"
#include "index/index.h"
#include "storage/protos.h"

extern int read_cache_size;
extern double cfl_require;
extern int rewriting_algorithm;

/* output of read_thread */
extern SyncQueue* read_queue;

/* chunk queue */
extern SyncQueue* chunk_queue;

/* hash queue */
extern SyncQueue* hash_queue;

/* output of prepare_thread */
extern SyncQueue* prepare_queue;

/* container queue */
extern SyncQueue* container_queue;

gboolean g_fingerprint_cmp(gconstpointer k1, gconstpointer k2)
{
    if (memcmp(k1, k2, sizeof(Fingerprint)) == 0)
        return TRUE;
    return FALSE;
}

void* rabin_chunk(void *arg) {
    int leftlen = 0;
    int left_offset = 0;
    unsigned char leftbuf[READ_BUFFER_SIZE + MAX_CHUNK_SIZE];
    int signal = 0;

    Jcr *jcr = (Jcr*) arg;

    while (TRUE) {
        Chunk *new_chunk = (Chunk*) malloc(sizeof(Chunk));
        new_chunk->duplicate = FALSE;
        new_chunk->container_id = TMP_CONTAINER_ID;
        new_chunk->length = 0;
        new_chunk->data = 0;

        if (signal >= 0 && leftlen < MAX_CHUNK_SIZE) {
            DataBuffer *data_buffer = sync_queue_pop(read_queue);
            /* save this signal */
            signal = data_buffer->size;
            if (signal > 0) {
                memmove(leftbuf, leftbuf + left_offset, leftlen);
                left_offset = 0;
                memcpy(leftbuf + leftlen, data_buffer->data, data_buffer->size);
                leftlen += data_buffer->size;
            }
            free(data_buffer);
        }

        if (leftlen > 0) {
            TIMER_DECLARE(b, e);
            TIMER_BEGIN(b);

            new_chunk->length = chunk_data(leftbuf + left_offset, leftlen);

            TIMER_END(jcr->chunk_time, b, e);

            new_chunk->data = (unsigned char*) malloc(new_chunk->length);
            memcpy(new_chunk->data, leftbuf + left_offset, new_chunk->length);
            leftlen -= new_chunk->length;
            left_offset += new_chunk->length;
            sync_queue_push(chunk_queue, new_chunk);
        } else {
            if (signal == FILE_END) {
                leftlen = 0;
                left_offset = 0;
                signal = 0;
                windows_reset();
                new_chunk->length = FILE_END;
                sync_queue_push(chunk_queue, new_chunk);
            } else if(signal == STREAM_END) {
                new_chunk->length = STREAM_END;
                new_chunk->data = 0;
                sync_queue_push(chunk_queue, new_chunk);
                break;
            }
        }
    }
    return NULL;
}

void* sha1_hash(void* arg) {
    Jcr *jcr = (Jcr*) arg;
    char zeros[64*1024];
    bzero(zeros, 64*1024);
    while (TRUE) {
        Chunk *chunk = sync_queue_pop(chunk_queue);
        if (chunk->length == STREAM_END) {
            sync_queue_push(hash_queue, chunk);
            break;
        }else if(chunk->length == FILE_END){
            sync_queue_push(hash_queue, chunk);
            continue;
        }
        if(memcmp(zeros, chunk->data, chunk->length) == 0){
            if(chunk->length != 64*1024)
                puts("unusual zero chunk");
            jcr->zero_chunk_count++;
            jcr->zero_chunk_amount += chunk->length;
        }

        TIMER_DECLARE(b, e);
        TIMER_BEGIN(b);
        SHA_CTX ctx;
        SHA_Init(&ctx);
        SHA_Update(&ctx, chunk->data, chunk->length);
        SHA_Final(chunk->hash, &ctx);
        TIMER_END(jcr->name_time, b, e);

        sync_queue_push(hash_queue, chunk);
    }
    return NULL;
}

/* 
 * Many fingerprint indexes do not need this process, like ram-index and ddfs.
 * But others, like extreme binning and SiLo, 
 * need this process to buffer some fingerprints to extract characteristic fingerprint.
 */
/* Some kinds of fingerprint index needs FILE_END signal, such as extreme binning */
void * simply_prepare(void *arg){
    Jcr *jcr = (Jcr*)arg;
    Recipe *processing_recipe = 0;
    while(TRUE){
        Chunk *chunk = sync_queue_pop(hash_queue);
        if(chunk->length == STREAM_END){
            sync_queue_push(prepare_queue, chunk);
            break;
        }
        if(processing_recipe == 0){
            processing_recipe = sync_queue_pop(jcr->waiting_files_queue);
            puts(processing_recipe->filename);
        }
        if(chunk->length == FILE_END){
            /* TO-DO */
            close(processing_recipe->fd);
            sync_queue_push(jcr->completed_files_queue, processing_recipe);
            processing_recipe = 0;
            free_chunk(chunk);
            /* FILE notion is meaningless for following threads */
            continue;
        }
        /* TO-DO */
        processing_recipe->chunknum++;
        processing_recipe->filesize += chunk->length;
        sync_queue_push(prepare_queue, chunk);
    }
    return NULL;
}

/*The typical dedup.*/
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

        new_fchunk->container_id = index_search(&chunk->hash, &chunk->feature);
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

/*
 * Handle containers in container_queue.
 * When a container buffer is full, we push it into container_queue.
 */
/*void* append_thread(void *arg){*/
    /*Jcr* jcr= (Jcr*)arg;*/
    /*while(TRUE){*/
        /*Container *container = sync_queue_pop(container_queue);*/
        /*if(container->id == STREAM_END){*/
            /*[> backup job finish <]*/
            /*container_free_full(container);*/
            /*break;*/
        /*}*/
        /*struct timeval begin, end;*/
        /*gettimeofday(&begin, 0);*/
        /*append_container(container);*/
        /*gettimeofday(&end, 0);*/
        /*jcr->write_time += (end.tv_sec - begin.tv_sec)*1000000 + end.tv_usec - begin.tv_usec;*/
        /*container_free_full(container);*/
    /*}*/
/*}*/

void free_chunk(Chunk* chunk){
    if(chunk->data && chunk->length>0)
        free(chunk->data);
    chunk->data = 0;
    free(chunk);
}

/* prepare thread for extreme binning */
/*
 * buffer all fingers of one file(free data part of chunks),
 * select the feature.
 */
void* exbin_prepare(void *arg){
    Jcr *jcr = (Jcr*)arg;
    Recipe *processing_recipe = 0;
    Fingerprint current_feature;
    memset(&current_feature, 0xff, sizeof(Fingerprint));
    Queue *buffered_chunk_queue = queue_new();
    while(TRUE){
        Chunk *chunk = sync_queue_pop(hash_queue);
        if(chunk->length == STREAM_END){
            sync_queue_push(prepare_queue, chunk);
            break;
        }
        if(processing_recipe == 0){
            processing_recipe = sync_queue_pop(jcr->waiting_files_queue);
            puts(processing_recipe->filename);
        }
        if(chunk->length == FILE_END){
            /* TO-DO */
            lseek(processing_recipe->fd, 0, SEEK_SET);
            while(queue_size(buffered_chunk_queue)){
                Chunk *buffered_chunk = queue_pop(buffered_chunk_queue);
                memcpy(&buffered_chunk->feature, &current_feature, sizeof(Fingerprint));
                buffered_chunk->data = malloc(buffered_chunk->length);
                read(processing_recipe->fd, buffered_chunk->data, buffered_chunk->length);
                sync_queue_push(prepare_queue, buffered_chunk);
            }
            memset(current_feature, 0xff, sizeof(Fingerprint));

            close(processing_recipe->fd);
            sync_queue_push(jcr->completed_files_queue, processing_recipe);
            processing_recipe = 0;
            /* FILE notion is meaningless for following threads */
            free_chunk(chunk);
            continue;
        }
        /* TO-DO */
        free(chunk->data);
        chunk->data = 0;
        if(memcmp(&current_feature, &chunk->hash, sizeof(Fingerprint))>0){
            memcpy(&current_feature, &chunk->hash, sizeof(Fingerprint));
        }
        processing_recipe->chunknum++;
        processing_recipe->filesize += chunk->length;

        queue_push(buffered_chunk_queue, chunk);
    }
    queue_free(buffered_chunk_queue, NULL);
    return NULL;
}

extern int32_t silo_segment_hash_size;
extern int32_t silo_item_size;

/*
 * prepare thread for SiLo.
 */
void* silo_prepare(void *arg){
    Jcr *jcr = (Jcr*)arg;
    Recipe *processing_recipe = 0;
    Fingerprint current_feature;
    memset(&current_feature, 0xff, sizeof(Fingerprint));
    Queue *buffered_chunk_queue = queue_new();
    int32_t current_segment_size = 0;
    while(TRUE){
        Chunk *chunk = sync_queue_pop(hash_queue);
        if(chunk->length == STREAM_END){
            if(current_segment_size > 0){
                /* process remaing chunks */
                Chunk *buffered_chunk = queue_pop(buffered_chunk_queue);
                while(buffered_chunk){
                    memcpy(&buffered_chunk->feature, &current_feature, sizeof(Fingerprint));
                    sync_queue_push(prepare_queue, buffered_chunk);
                    buffered_chunk = queue_pop(buffered_chunk_queue);
                }
            }
            sync_queue_push(prepare_queue, chunk);
            break;
        }
        if(processing_recipe == 0){
            processing_recipe = sync_queue_pop(jcr->waiting_files_queue);
            puts(processing_recipe->filename);
        }
        if(chunk->length == FILE_END){
            /* TO-DO */
            close(processing_recipe->fd);
            sync_queue_push(jcr->completed_files_queue, processing_recipe);
            processing_recipe = 0;
            free_chunk(chunk);
            /* FILE notion is meaningless for following threads */
            continue;
        }
        /* TO-DO */
        if((current_segment_size+silo_item_size) > silo_segment_hash_size){
            /* segment is full, push it */
            Chunk *buffered_chunk = queue_pop(buffered_chunk_queue);
            while(buffered_chunk){
                memcpy(&buffered_chunk->feature, &current_feature, sizeof(Fingerprint));
                sync_queue_push(prepare_queue, buffered_chunk);
                buffered_chunk = queue_pop(buffered_chunk_queue);
            }
            current_segment_size = 0;
            memset(&current_feature, 0xff, sizeof(Fingerprint));
        }
        if(memcmp(&current_feature, &chunk->hash, sizeof(Fingerprint)) > 0){
            memcpy(&current_feature, &chunk->hash, sizeof(Fingerprint));
        }
        processing_recipe->chunknum++;
        processing_recipe->filesize += chunk->length;

        queue_push(buffered_chunk_queue, chunk);
        current_segment_size += silo_item_size;
    }
    queue_free(buffered_chunk_queue, NULL);
    return NULL;
}
