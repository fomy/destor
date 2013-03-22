#ifndef CONTAINER_CACHE_H
#define CONTAINER_CACHE_H

#include "container.h"
#include "../tools/lru_cache.h"
#include "../tools/queue.h"
#include "cfl_monitor.h"

typedef struct container_cache_tag ContainerCache;

/*
 * Maintain map as finger-list pair for rewriting algorithm.
 */
struct container_cache_tag{
    LRUCache *lru_cache;
    GHashTable *map;// finger:container-list pairs
    BOOL enable_data;
    CFLMonitor* cfl_monitor;
};

/* container_cache.c */
ContainerCache* container_cache_new(int cache_size, BOOL enable_data);
CFLMonitor* container_cache_free(ContainerCache *cc);
BOOL container_cache_contains(ContainerCache *cc, Fingerprint *finger);
Container* container_cache_lookup(ContainerCache *cc, Fingerprint *finger);
Container* container_cache_insert_container(ContainerCache *cc, ContainerId cid);
Chunk *container_cache_get_chunk(ContainerCache *cc, Fingerprint* finger, ContainerId container_id);

#endif
