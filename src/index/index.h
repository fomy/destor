#ifndef INDEX_H_
#define INDEX_H_

#include "../global.h"

BOOL index_init();
void index_destroy();
/*
 * Search in Fingerprint Index.
 * Return TMP_CONTAINER_ID if not exist.
 * Return old ContainerId if exist.
 */
ContainerId index_search(Fingerprint* finger, void* feature);
/*
 * Insert fingerprint into Index for new fingerprint or new ContainerId.
 */
void index_update(Fingerprint*, ContainerId, void* feature, BOOL update);
void* exbin_prepare(void *arg);
void* silo_prepare(void *arg);
#endif
