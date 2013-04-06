/*
 * container_cache.c
 *
 *  Created on: Sep 21, 2012
 *      Author: fumin
 */

#include "container_cache.h"
#include "container_volume.h"
#include "../dedup.h"

/*
 * seed_file will be ingnored when cache_type is LRU_CACHE
 */
ContainerCache *container_cache_new(int cache_size, BOOL enable_data_cache)
{
    ContainerCache *cc = (ContainerCache *)malloc(sizeof(ContainerCache));
    cc->lru_cache = lru_cache_new(cache_size, container_cmp_asc);
    cc->enable_data = enable_data_cache;
    if (cc->enable_data)
        cc->cfl_monitor = cfl_monitor_new(cache_size);
    else
        cc->cfl_monitor = 0;
    cc->map = g_hash_table_new_full(g_int64_hash,
            g_fingerprint_cmp, free, g_sequence_free);
    return cc;
}

CFLMonitor *container_cache_free(ContainerCache *cc)
{
    CFLMonitor *monitor = cc->cfl_monitor;
    g_hash_table_destroy(cc->map);
    lru_cache_free(cc->lru_cache, (void (*)(void*))container_free_full);
    free(cc);
    return monitor;
}

BOOL container_cache_contains(ContainerCache *cc, Fingerprint *finger)
{
    Container *container = container_cache_lookup(cc, finger);
    return container != 0 ? TRUE : FALSE;
}

Container* container_cache_lookup(ContainerCache *cc, Fingerprint *finger)
{
    Container *container = 0;
    GSequence *container_list = g_hash_table_lookup(cc->map, finger);
    if (container_list)
    {
        container = g_sequence_get(g_sequence_get_begin_iter(container_list));
        if (!lru_cache_lookup(cc->lru_cache, container))
        {
            printf("%s, %d: inconsistency between map and cache.\n", __FILE__, __LINE__);
        }
    }
    return container;
}

/* used in restore */
Chunk *container_cache_get_chunk(ContainerCache *cc, 
        Fingerprint *finger, ContainerId container_id)
{
    Container *container = container_cache_lookup(cc, finger);
    Chunk *result = 0;
    if (container)
    {
        result = container_get_chunk(container, finger);
        update_cfl_directly(cc->cfl_monitor, result->length, FALSE);
    }
    else
    {
        container = container_cache_insert_container(cc, container_id);
        result = container_get_chunk(container, finger);
        update_cfl_directly(cc->cfl_monitor, result->length, TRUE);
    }
    return result;
}

Container *container_cache_insert_container(ContainerCache *cc, 
        ContainerId cid)
{
    /* read container */
    Container *container = 0;
    if (cc->enable_data)
    {
#ifdef SIMULATOR
        container = read_container_meta_only(cid);
#else
        container = read_container(cid);
#endif
    }
    else
    {
        container = read_container_meta_only(cid);
    }
    /* If this container is newly appended,
     * maybe we can read nothing. */
    if (container == NULL)
        return NULL;

    /* insert */
    Container *evictor = lru_cache_insert(cc->lru_cache, container);

    /* evict */
    if (evictor)
    {
        int32_t chunknum = container_get_chunk_num(evictor);
        Fingerprint *fingers = container_get_all_fingers(evictor);
        int i = 0;
        /* evict all fingers of evictor from map */
        for (; i < chunknum; ++i)
        {
            GSequence* container_list = g_hash_table_lookup(cc->map, &fingers[i]);
            /* remove the specified container from list */
            GSequenceIter *iter = g_sequence_lookup(container_list, evictor, container_cmp_des, NULL);
            if(iter)
                g_sequence_remove(iter);
            else
                dprint("Error! The sequence does not contain the container.");
            if(g_sequence_get_length(container_list) == 0){
                g_hash_table_remove(cc->map, &fingers[i]);
            }
        }
        free(fingers);
        container_free_full(evictor);
    }

    /* insert */
    int32_t num = container_get_chunk_num(container);
    Fingerprint *nfingers = container_get_all_fingers(container);
    int i = 0;
    for (; i < num; ++i)
    {
        GSequence* container_list = g_hash_table_lookup(cc->map, &nfingers[i]);
        if(container_list == 0){
            container_list = g_sequence_new(NULL);
            Fingerprint *finger = (Fingerprint *)malloc(sizeof(Fingerprint));
            memcpy(finger, &nfingers[i], sizeof(Fingerprint));
            g_hash_table_insert(cc->map, finger, container_list);
        }
        g_sequence_insert_sorted(container_list, container, container_cmp_des, NULL);
    }
    free(nfingers);
    return container;
}
