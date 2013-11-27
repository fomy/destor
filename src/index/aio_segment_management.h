/*
 * aio_segment_management.h
 *
 *  Created on: Nov 23, 2013
 *      Author: fumin
 */

#ifndef AIO_SEGMENT_MANAGEMENT_H_
#define AIO_SEGMENT_MANAGEMENT_H_

#include "index.h"

void init_aio_segment_management();

void close_aio_segment_management();

/*
 * Each segment calls the function.
 * If id is TEMPORARY_ID, no similar segment is found.
 */
struct segmentRecipe* retrieve_segment_all_in_one(segmentid id);

/*
 * Return the joint segment recipe.
 */
struct segmentRecipe* update_segment_all_in_one(struct segmentRecipe*);

#endif /* AIO_SEGMENT_MANAGEMENT_H_ */
