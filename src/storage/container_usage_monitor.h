#ifndef USAGE_MONITOR_H
#define USAGE_MONITOR_H

#include "../global.h"
#include "container.h"
#include "../tools/queue.h"

typedef struct container_usage_monitor_tag ContainerUsageMonitor;
typedef struct container_usage_tag ContainerUsage;

struct container_usage_monitor_tag{
    /* Those containers of larger usage 
     * are put into this map. */
    /* container_id : size */
    GHashTable *dense_map;
    /* Those containers of smaller usage,
     * smaller than threshold, 
     * are put into this map. */
    /* container_id :sparse container */
    GHashTable *sparse_map;

    int64_t total_size;
};

struct container_usage_tag{
    ContainerId cntnr_id;
    int32_t read_size;
};

ContainerUsageMonitor* container_usage_monitor_new();
void container_usage_monitor_free(ContainerUsageMonitor* cntnr_usg_mntr);
void container_usage_monitor_update(ContainerUsageMonitor* cntnr_usge_mntr, 
        ContainerId cntnr_id, Fingerprint *fngr, int32_t len);
int container_usage_monitor_print(ContainerUsageMonitor *cntnr_usg_mntr, int32_t job_id,
        GHashTable* old_sparse_containers);

GHashTable* load_historical_sparse_containers(int32_t job_id);
void destroy_historical_sparse_containers(GHashTable* sparse_containers);
#endif
