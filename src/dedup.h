/*
 * dedup.h
 *
 *  Created on: Sep 21, 2012
 *      Author: fumin
 */

#ifndef DEDUP_H_
#define DEDUP_H_

#include "global.h"

#define UNIQUE (0x00)
#define DUPLICATE (0x01)
#define SPARSE (0x02)
#define OUT_OF_ORDER (0x04)
#define NOT_IN_CACHE (0x08)

typedef struct chunk_tag Chunk;
typedef struct data_buffer_tag DataBuffer;

/* buffer for read_thread */
struct data_buffer_tag {
	int32_t size;
	unsigned char data[READ_BUFFER_SIZE];
};

struct chunk_tag {
	int32_t length;
	unsigned char *data;
	//BOOL duplicate;
    int status;
	Fingerprint hash;
    /* for SiLo and Extreme Binning */
    void *feature;
    ContainerId container_id;
};

Chunk* allocate_chunk();
void free_chunk(Chunk* chunk);
gboolean g_fingerprint_cmp(gconstpointer k1, 
        gconstpointer k2);
#endif /* DEDUP_H_ */
