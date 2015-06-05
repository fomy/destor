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

#include "../destor.h"

/*
 * The function is used to initialize memory structures of a fingerprint index.
 */
void init_index();
/*
 * Free memory structures and flush them into disks.
 */
void close_index();
/*
 * lookup fingerprints in a segment in index.
 */
int index_lookup(struct segment*);
/*
 * Insert/update fingerprints.
 */
void index_update(GHashTable *features, int64_t id);

void index_delete(fingerprint *fp, int64_t id);

void index_check_buffer(struct segment *s);
int index_update_buffer(struct segment *s);

//void index_delete(fingerprint *);

extern GHashTable* (*sampling)(GSequence *chunks, int32_t chunk_num);
extern struct segment* (*segmenting)(struct chunk *c);

gboolean g_feature_equal(char* a, char* b);
guint g_feature_hash(char *feature);

#endif
