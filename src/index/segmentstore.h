/*
 * segmentstore.h
 *
 *  Created on: Nov 24, 2013
 *      Author: fumin
 */

#ifndef SEGMENTSTORE_H_
#define SEGMENTSTORE_H_

#include "index.h"

void init_segment_management();

void close_segment_management();

struct segmentRecipe* retrieve_segment(segmentid);

struct segmentRecipe* update_segment(struct segmentRecipe*);

#endif /* SEGMENTSTORE_H_ */
