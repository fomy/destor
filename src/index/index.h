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
ContainerId index_search(Fingerprint* finger, void* eigenvalue);
/*
 * Insert fingerprint into Index for new fingerprint or new ContainerId.
 */
void index_insert(Fingerprint*, ContainerId, void* eigenvalue);
#endif
