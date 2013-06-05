#ifndef SILO_H_
#define SILO_H_

#include "../global.h"
#include "../dedup.h"
#include "htable.h"

typedef struct silo_block_tag SiloBlock;
typedef struct silo_segment_tag SiloSegment;

/* locality unit */
struct silo_block_tag {
	/* fingerprint to container id */
	HTable *LHTable;
	/* rep_fingers of all segments in this block */
	GHashTable* representative_table; //for write_buffer
	int32_t id;
	int32_t size;
	BOOL dirty;
};

/* similarity unit */
struct silo_segment_tag {
	Fingerprint delegate;
	/* finger-container_id pairs */
	GHashTable *fingers;
};

void silo_destroy();
BOOL silo_init();
ContainerId silo_search(Fingerprint* fingerprint, EigenValue *eigenvalue);
void silo_update(Fingerprint* fingerprint, ContainerId containerId,
		EigenValue* eigenvalue, BOOL update);

#endif
