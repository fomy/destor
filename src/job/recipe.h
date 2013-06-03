/*
 * recipe.h
 *
 *  Created on: Sep 21, 2012
 *      Author: fumin
 */

#ifndef RECIPE_H_
#define RECIPE_H_

#include "../global.h"
#include "../dedup.h"

typedef struct recipe_tag Recipe;
typedef struct finger_chunk_tag FingerChunk;

struct finger_chunk_tag {
	Fingerprint fingerprint;
	ContainerId container_id;
	int length; //only used for optimal cache.
	struct finger_chunk_tag *next;
};

struct recipe_tag {
	int32_t fileindex;
	int32_t chunknum;
	int64_t filesize;
	char *filename;
	int fd;
};

Recipe* recipe_new();
Recipe* recipe_new_full(char * filename);
void recipe_free(Recipe* trash);
#endif /* RECIPE_H_ */
