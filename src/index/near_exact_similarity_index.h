/*
 * near_exact_similarity_index.h
 *
 *  Created on: Nov 19, 2013
 *      Author: fumin
 */

#ifndef NEAR_EXACT_SIMILARITY_INDEX_H_
#define NEAR_EXACT_SIMILARITY_INDEX_H_

#include "../destor.h"

void init_near_exact_similarity_index();
void close_near_exact_similarity_index();
void near_exact_similarity_index_lookup(struct segment* s);
containerid near_exact_similarity_index_update(fingerprint *fp, containerid from,
		containerid to);

#endif /* NEAR_EXACT_SIMILARITY_INDEX_H_ */
