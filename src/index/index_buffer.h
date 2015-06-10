#ifndef INDEX_BUFFER_H_
#define INDEX_BUFFER_H_

#include "../destor.h"
/*
 * The basic unit in index buffer.
 */
struct indexElem {
    containerid id;
    fingerprint fp;
};

/* The buffer size > 2 * destor.rewrite_buffer_size */
/* All fingerprints that have been looked up in the index
 * but not been updated. */
struct index_buffer {
    /* map a fingerprint to a queue of indexElem */
    /* Index all fingerprints in the index buffer. */
    GHashTable *buffered_fingerprints;
    /* The number of buffered chunks */
    int chunk_num;
};

struct index_overhead {
    /* Requests to the key-value store */
    int lookup_requests;
    int update_requests;
    int lookup_requests_for_unique;
    /* Overheads of prefetching module */
    int read_prefetching_units;
};

#endif
