#include "global.h"
#include "dedup.h" 
#include "jcr.h"
#include "tools/sync_queue.h"
#include "index/index.h"

/* hash queue */
extern SyncQueue* hash_queue;

/* output of prepare_thread */
extern SyncQueue* prepare_queue;

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
        /*chunk->container_id = index_search(&chunk->hash, &chunk->feature);*/
        sync_queue_push(prepare_queue, chunk);
    }
    return NULL;
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
                /*buffered_chunk->container_id = index_search(&buffered_chunk->hash, &buffered_chunk->feature);*/
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
                    /*buffered_chunk->container_id = index_search(&buffered_chunk->hash, &buffered_chunk->feature);*/
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
                /*buffered_chunk->container_id = index_search(&buffered_chunk->hash, &buffered_chunk->feature);*/
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
