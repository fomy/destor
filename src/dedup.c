#include "dedup.h"

void free_chunk(Chunk* chunk){
    if(chunk == NULL)
        return;
    if(chunk->data && chunk->length>0)
        free(chunk->data);
    chunk->data = 0;
    free(chunk);
}

gboolean g_fingerprint_cmp(gconstpointer k1, gconstpointer k2)
{
    if (memcmp(k1, k2, sizeof(Fingerprint)) == 0)
        return TRUE;
    return FALSE;
}
