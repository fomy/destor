#include "global.h"
#include "dedup.h" 
#include "jcr.h"
#include "tools/sync_queue.h"

/* chunk queue */
extern SyncQueue* chunk_queue;

/* hash queue */
extern SyncQueue* hash_queue;

gboolean g_fingerprint_cmp(gconstpointer k1, gconstpointer k2)
{
    if (memcmp(k1, k2, sizeof(Fingerprint)) == 0)
        return TRUE;
    return FALSE;
}

void* sha1_hash(void* arg) {
    Jcr *jcr = (Jcr*) arg;
    while (TRUE) {
        Chunk *chunk = sync_queue_pop(chunk_queue);
        if (chunk->length == STREAM_END) {
            sync_queue_push(hash_queue, chunk);
            break;
        }else if(chunk->length == FILE_END){
            sync_queue_push(hash_queue, chunk);
            continue;
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
