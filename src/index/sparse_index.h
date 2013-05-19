#ifndef SPARSE_H_
#define SPARSE_H_

#include "index.h"

typedef struct manifest {
	int64_t id; //first 5 bytes are address, last 3 bytes are length
	GSequence *matched_hooks; //Fingerprint sequence
	GHashTable *fingers;
} Manifest;

typedef struct hooks {
	int32_t size;
	Fingerprint hooks[0];
} Hooks;

BOOL sparse_index_init();
void sparse_index_destroy();
ContainerId sparse_index_search(Fingerprint *fingerprint,
		EigenValue *eigenvalue);
void sparse_index_update(Fingerprint *fingerprint, ContainerId container_id,
		EigenValue *eigenvalue, BOOL update);
#endif
