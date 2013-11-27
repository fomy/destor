/*
 * exact_similarity_index.h
 *
 *  Created on: Nov 19, 2013
 *      Author: fumin
 */

#ifndef EXACT_SIMILARITY_INDEX_H_
#define EXACT_SIMILARITY_INDEX_H_

#include "../destor.h"

void init_exact_similarity_index();
void close_exact_similarity_index();
void exact_similarity_index_lookup(struct segment* s);
containerid exact_similarity_index_update(fingerprint fp, containerid from,
		containerid to);

#endif
