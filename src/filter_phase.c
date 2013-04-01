#include "global.h"
#include "dedup.h" 
#include "jcr.h"
#include "index/index.h"
#include "storage/protos.h"

extern int rewriting_algorithm;

extern int recv_feature(Chunk **chunk);
extern ContainerId save_chunk(Chunk* chunk);

static pthread_t filter_t;

extern void send_fc_signal();
extern void send_fingerchunk(FingerChunk *fchunk, 
        Fingerprint *feature, BOOL update);

extern void* cfl_filter(void* arg);
extern void* cbr_filter(void* arg);
extern void* cap_filter(void* arg);

static void* simply_filter(void* arg){
    Jcr* jcr = (Jcr*) arg;
    GHashTable* historical_sparse_containers = 0;
    historical_sparse_containers = load_historical_sparse_containers(jcr->job_id);
    ContainerUsageMonitor* monitor =container_usage_monitor_new();
    while (TRUE) {
        Chunk* chunk = NULL;
        int signal = recv_feature(&chunk);

        TIMER_DECLARE(b1, e1);
        TIMER_BEGIN(b1);
        if (signal == STREAM_END) {
            free_chunk(chunk);
            break;
        }

        /* init FingerChunk */
        FingerChunk *new_fchunk = (FingerChunk*)malloc(sizeof(FingerChunk));
        new_fchunk->length = chunk->length;
        memcpy(&new_fchunk->fingerprint, &chunk->hash, sizeof(Fingerprint));
        new_fchunk->container_id = chunk->container_id;

        BOOL update = FALSE;
        if (new_fchunk->container_id != TMP_CONTAINER_ID) {
            if(rewriting_algorithm == HBR_REWRITING && historical_sparse_containers!=0 && 
                    g_hash_table_lookup(historical_sparse_containers, &new_fchunk->container_id) != NULL){
                /* this chunk is in a sparse container */
                /* rewrite it */
                chunk->duplicate = FALSE;
                new_fchunk->container_id = save_chunk(chunk);

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
            new_fchunk->container_id = save_chunk(chunk); 
            update = TRUE;
        }
        container_usage_monitor_update(monitor, new_fchunk->container_id,
                &new_fchunk->fingerprint, new_fchunk->length);
        send_fingerchunk(new_fchunk, &chunk->feature, update);
        TIMER_END(jcr->filter_time, b1, e1);
        free_chunk(chunk);
    }//while(TRUE) end

    save_chunk(NULL);

    send_fc_signal();

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

int start_filter_phase(Jcr *jcr){
    if (rewriting_algorithm == NO_REWRITING) {
        puts("rewriting_algorithm=NO");
        pthread_create(&filter_t, NULL, simply_filter, jcr);
    } else if(rewriting_algorithm == CFL_REWRITING){
        puts("rewriting_algorithm=CFL");
        pthread_create(&filter_t, NULL, cfl_filter, jcr);
    } else if(rewriting_algorithm == CBR_REWRITING){
        puts("rewriting_algorithm=CBR");
        pthread_create(&filter_t, NULL, cbr_filter, jcr);
    } else if(rewriting_algorithm == HBR_REWRITING){
        puts("rewriting_algorithm=HBR");
        pthread_create(&filter_t, NULL, simply_filter, jcr);
    } else if(rewriting_algorithm == HBR_CBR_REWRITING){
        puts("rewriting_algorithm=HBR_CBR");
        pthread_create(&filter_t, NULL, cbr_filter, jcr);
    } else if(rewriting_algorithm == CAP_REWRITING){
        puts("rewriting_algorithm=CAP");
        pthread_create(&filter_t, NULL, cap_filter, jcr);
    } else if(rewriting_algorithm == HBR_CAP_REWRITING){
        puts("rewriting_algorithm=HBR_CAP");
        pthread_create(&filter_t, NULL, cap_filter, jcr);
    } else{
        dprint("invalid rewriting algorithm\n");
        return FAILURE;
    }
}

void stop_filter_phase(){
    pthread_join(filter_t, NULL);
}
