#ifndef OPT_CACHE_H_
#define OPT_CACHE_H_

#include "container.h"
#include "../tools/queue.h"
#include "cfl_monitor.h"

typedef struct seed Seed;
typedef struct container_opt_cache OptimalContainerCache;

struct seed{ //8 bytes
    ContainerId container_id;
    int32_t remaining_size;
};

typedef struct distance_item DistanceItem;
typedef struct container_pool ContainerPool;

struct distance_item{
    ContainerId container_id;
    int32_t next_position;
    int32_t furthest_position;
    Queue *distance_queue;//distance queue
};

struct container_pool{
    /* distance_item sequence */
    GSequence *distance_sequence;
    /* container pool */
    GHashTable *pool;
};

/*
 * Each container will be assigned a DistanceItem.
 */
struct container_opt_cache{
    Seed *active_seed;

    int cache_size;
    BOOL enable_data;
    int current_distance;

    FILE *seed_file;
    /* slide window */
    BOOL seed_file_is_end;
    //int total_seed_count;
    /* window */
    int seed_count;// current seeds in buffer
    Seed *seed_buffer;
    int window_start;
    int window_size;
    /* distances of seeds in window */

    GHashTable *distance_table;

    ContainerPool *container_pool;
    /* finger and container-list map */
    GHashTable *map;

    CFLMonitor* cfl_monitor;
};

OptimalContainerCache* optimal_container_cache_new(int cache_size, 
        BOOL enable_data, FILE* seed_file, int window_size);
CFLMonitor* optimal_container_cache_free(OptimalContainerCache *opt_cache);
Chunk* optimal_container_cache_get_chunk(OptimalContainerCache* opt_cache,
        Fingerprint *finger, ContainerId container_id);
#endif
