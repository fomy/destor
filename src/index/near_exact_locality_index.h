/*
 * near_exact_locality_index.h
 *
 *  Created on: Nov 15, 2013
 *      Author: fumin
 */

#ifndef NEAR_EXACT_LOCALITY_INDEX_H_
#define NEAR_EXACT_LOCALITY_INDEX_H_

#include "../destor.h"

void init_near_exact_locality_index();

void close_near_exact_locality_index();

void near_exact_locality_index_lookup(struct segment*);

containerid near_exact_locality_index_update(fingerprint *fp, containerid from,
		containerid to);

#endif /* NEAR_EXACT_LOCALITY_INDEX_H_ */
