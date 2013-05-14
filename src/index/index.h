/*
 * An index_update() to a fingerprint will be called after the index_search() to the fingerprint.
 * However, due to the existence of rewriting algorithms,
 * there is no guarantee that the index_update() will be called immediately after the index_search().
 * Thus, we would better make them independent with each other.
 *
 * The input of index_search is nearly same as that of index_update() except the ContainerId field.
 */
#ifndef INDEX_H_
#define INDEX_H_

#include "../global.h"
#include "../dedup.h"
/*
 * The function is used to initialize memory structures of a fingerprint index.
 */
BOOL index_init();
/*
 * Free memory structures and flush them into disks.
 */
void index_destroy();
/*
 * Search in Fingerprint Index.
 * Return TMP_CONTAINER_ID if not exist.
 * Return old ContainerId if exist.
 */
ContainerId index_search(Fingerprint* finger, EigenValue *eigenvalue);
/*
 * Insert fingerprint into Index for new fingerprint or new ContainerId.
 */
void index_update(Fingerprint*, ContainerId, EigenValue* eigenvalue,
		BOOL update);

EigenValue* extract_eigenvalue_exbin(Chunk *chunk);
EigenValue* extract_eigenvalue_sparse(Chunk* chunk);
EigenValue* extract_eigenvalue_silo(Chunk *chunk);
#endif
