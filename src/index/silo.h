#ifndef SILO_H_
#define SILO_H_

#include "../global.h"
#include "../dedup.h"

typedef struct silo_block_tag SiloBlock;
typedef struct silo_segment_tag SiloSegment;

/* locality unit */
struct silo_block_tag{
    /* fingerprint to container id */
    GHashTable *LHTable;
    /* rep_fingers of all segments in this block */
    GHashTable* delegate_table;//for write_buffer
    //Fingerprint current_segment;//for read_cache
    int32_t id;
    int32_t size;
};

/* similarity unit */
struct silo_segment_tag{
    Fingerprint delegate;
    /* finger-container_id pairs */
    GHashTable *fingers;
};

void silo_destroy();
BOOL silo_init();
ContainerId silo_search(Fingerprint* fingerprint, Fingerprint* delegate);
void silo_insert(Fingerprint* fingerprint, ContainerId containerId,
        Fingerprint *delegate);

#endif
