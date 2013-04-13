#include "global.h"
#include "dedup.h" 
#include "jcr.h"
#include "tools/sync_queue.h"
#include "index/index.h"
#include "storage/cfl_monitor.h"

extern int fingerprint_index_type;
extern BOOL enable_hbr;
extern BOOL enable_cache_filter;
extern int recv_hash(Chunk **chunk);
extern CFLMonitor* cfl_monitor;
/* output of prepare_thread */
static SyncQueue* feature_queue;
static pthread_t prepare_t;
static GHashTable *sparse_containers;

int sparse_chunk_count = 0;
int64_t sparse_chunk_amount = 0;
int in_cache_count = 0;
int64_t in_cache_amount = 0;

static void send_feature(Chunk *chunk){
    sync_queue_push(feature_queue, chunk);
}

int recv_feature(Chunk **new_chunk){
    Chunk *chunk = sync_queue_pop(feature_queue);
    if(chunk->length == FILE_END){
        *new_chunk = chunk;
        return FILE_END;
    }else if(chunk->length == STREAM_END){
        *new_chunk = chunk;
        return STREAM_END;
    }
    chunk->container_id = index_search(&chunk->hash, &chunk->feature);
    if(chunk->container_id != TMP_CONTAINER_ID){
        chunk->status |= DUPLICATE;
        if(enable_hbr && sparse_containers && g_hash_table_lookup(sparse_containers, 
                    &chunk->container_id) != NULL){
            ++sparse_chunk_count;
            sparse_chunk_amount += chunk->length;
            chunk->status |= SPARSE;
        }
        if(!enable_cache_filter || 
                is_container_already_in_cache(cfl_monitor, chunk->container_id)==FALSE){
            chunk->status |= NOT_IN_CACHE;
        }else{
            in_cache_count++;
            in_cache_amount += chunk->length;
        }
    }

    *new_chunk = chunk;
    return SUCCESS;
}

/* 
 * Many fingerprint indexes do not need this process, like ram-index and ddfs.  
 * But others, like extreme binning and SiLo, 
 * need this process to buffer some fingerprints to extract characteristic fingerprint.
 */
/* 
 * Some kinds of fingerprint index needs FILE_END signal, such as extreme binning 
 * */
void * simply_prepare(void *arg){
    Jcr *jcr = (Jcr*)arg;
    Recipe *processing_recipe = 0;
    while(TRUE){
        Chunk *chunk = NULL;
        int signal = recv_hash(&chunk);
        if(signal == STREAM_END){
            send_feature(chunk);
            break;
        }
        if(processing_recipe == 0){
            processing_recipe = sync_queue_pop(jcr->waiting_files_queue);
            puts(processing_recipe->filename);
        }
        if(signal == FILE_END){
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
        send_feature(chunk);
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
        Chunk *chunk = NULL;
        int signal = recv_hash(&chunk);
        if(signal == STREAM_END){
            send_feature(chunk);
            break;
        }
        if(processing_recipe == 0){
            processing_recipe = sync_queue_pop(jcr->waiting_files_queue);
            puts(processing_recipe->filename);
        }
        if(signal == FILE_END){
            /* TO-DO */
            lseek(processing_recipe->fd, 0, SEEK_SET);
            while(queue_size(buffered_chunk_queue)){
                Chunk *buffered_chunk = queue_pop(buffered_chunk_queue);
                memcpy(&buffered_chunk->feature, &current_feature, sizeof(Fingerprint));
                buffered_chunk->data = malloc(buffered_chunk->length);
                read(processing_recipe->fd, buffered_chunk->data, buffered_chunk->length);
                send_feature(buffered_chunk);
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
        Chunk *chunk = NULL;
        int signal = recv_hash(&chunk);
        if(signal == STREAM_END){
            if(current_segment_size > 0){
                /* process remaing chunks */
                Chunk *buffered_chunk = queue_pop(buffered_chunk_queue);
                while(buffered_chunk){
                    memcpy(&buffered_chunk->feature, &current_feature, sizeof(Fingerprint));
                    send_feature(buffered_chunk);
                    buffered_chunk = queue_pop(buffered_chunk_queue);
                }
            }
            send_feature(chunk);
            break;
        }
        if(processing_recipe == 0){
            processing_recipe = sync_queue_pop(jcr->waiting_files_queue);
            puts(processing_recipe->filename);
        }
        if(signal == FILE_END){
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
                send_feature(buffered_chunk);
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

int start_prepare_phase(Jcr *jcr){
    feature_queue = sync_queue_new(100);
    jcr->historical_sparse_containers = load_historical_sparse_containers(jcr->job_id);
    sparse_containers = jcr->historical_sparse_containers;
    sparse_chunk_count = 0;
    sparse_chunk_amount = 0;
    switch(fingerprint_index_type){
        case RAM_INDEX:
        case DDFS_INDEX:
            pthread_create(&prepare_t, NULL, simply_prepare, jcr);
            break;
        case EXBIN_INDEX:
            pthread_create(&prepare_t, NULL, exbin_prepare, jcr);
            break;
        case SILO_INDEX:
            pthread_create(&prepare_t, NULL, silo_prepare, jcr);
            break;
        default:
            dprint("wrong index type!");
    }
}

void stop_prepare_phase(){
    pthread_join(prepare_t, NULL);
    if(sparse_containers)
        destroy_historical_sparse_containers(sparse_containers);
}
