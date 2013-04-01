#include "global.h"
#include "dedup.h" 
#include "jcr.h"
#include "tools/sync_queue.h"

extern int recv_chunk(Chunk** chunk);

/* hash queue */
static SyncQueue* hash_queue;
static pthread_t hash_t;

gboolean g_fingerprint_cmp(gconstpointer k1, gconstpointer k2)
{
    if (memcmp(k1, k2, sizeof(Fingerprint)) == 0)
        return TRUE;
    return FALSE;
}

static void send_hash(Chunk* chunk){
    sync_queue_push(hash_queue, chunk);
}

int recv_hash(Chunk **new_chunk){
    Chunk *chunk = sync_queue_pop(hash_queue);
    if(chunk->length == FILE_END){
        /*free_chunk(chunk);*/
        *new_chunk = chunk;
        return FILE_END;
    }else if(chunk->length == STREAM_END){
        /*free_chunk(chunk);*/
        *new_chunk = chunk;
        return STREAM_END;
    }
    *new_chunk = chunk;
    return SUCCESS;
}

static void* sha1_thread(void* arg) {
    Jcr *jcr = (Jcr*) arg;
    while (TRUE) {
        Chunk *chunk = NULL;
        int signal = recv_chunk(&chunk);
        if (signal == STREAM_END) {
            send_hash(chunk);
            break;
        }else if(signal == FILE_END){
            send_hash(chunk);
            continue;
        }

        TIMER_DECLARE(b, e);
        TIMER_BEGIN(b);
        SHA_CTX ctx;
        SHA_Init(&ctx);
        SHA_Update(&ctx, chunk->data, chunk->length);
        SHA_Final(chunk->hash, &ctx);
        TIMER_END(jcr->name_time, b, e);

        send_hash(chunk);
    }
    return NULL;
}

int start_hash_phase(Jcr *jcr){
    hash_queue = sync_queue_new(100);
    pthread_create(&hash_t, NULL, sha1_thread, jcr);
}

void stop_hash_phase(){
    pthread_join(hash_t, NULL);
}
