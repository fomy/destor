/*
 * segment_management.h
 *
 *  Created on: Nov 24, 2013
 *      Author: fumin
 */

#ifndef SEGMENT_MANAGEMENT_H_
#define SEGMENT_MANAGEMENT_H_

#include "index.h"

void init_segment_management();

void close_segment_management();

struct segmentRecipe* retrieve_segment(segmentid);

struct segmentRecipe* update_segment(struct segmentRecipe*);

#endif /* SEGMENT_MANAGEMENT_H_ */
