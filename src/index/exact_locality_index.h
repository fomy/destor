/*
 * exact_locality_index.h
 *
 *  Created on: Nov 19, 2013
 *      Author: fumin
 */

#ifndef EXACT_LOCALITY_INDEX_H_
#define EXACT_LOCALITY_INDEX_H_

#include "../destor.h"

void init_exact_locality_index();
void close_exact_locality_index();
void exact_locality_index_lookup(struct segment* s);
containerid exact_locality_index_update(fingerprint fp, containerid from,
		containerid to);

#endif /* EXACT_LOCALITY_INDEX_H_ */
