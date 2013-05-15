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

typedef struct {
	int value_num;
	/* indicates how many chunks there are in the segment. */
	int chunk_num;
	Fingerprint values[0];
} EigenValue;

struct chunk_tag {
	int32_t length;
	unsigned char *data;
	int status;
	Fingerprint hash;
	EigenValue *eigenvalue;
	ContainerId container_id;
};

Chunk* allocate_chunk();
void free_chunk(Chunk* chunk);
gboolean g_fingerprint_equal(gconstpointer k1, gconstpointer k2);
gboolean g_fingerprint_cmp(gconstpointer k1, gconstpointer k2,
		gpointer user_data);
void print_finger(Fingerprint *fingerprint);
#endif /* DEDUP_H_ */
