#ifndef SPARSE_H_
#define SPARSE_H_

#include "index.h"

BOOL sparse_index_init();
void sparse_index_destroy();
ContainerId sparse_index_search(Fingerprint *fingerprint,
		EigenValue *eigenvalue);
void sparse_index_update(Fingerprint *fingerprint, ContainerId container_id,
		EigenValue *eigenvalue, BOOL update);
#endif
