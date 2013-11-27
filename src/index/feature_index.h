/*
 * feature_index.h
 *
 *  Created on: Nov 21, 2013
 *      Author: fumin
 */

#ifndef FEATURE_INDEX_H_
#define FEATURE_INDEX_H_

#include "../destor.h"

void init_feature_index();
void close_feature_index();
GQueue* feature_index_lookup(fingerprint *feature);
segmentid feature_index_lookup_for_latest(fingerprint *feature);
void feature_index_update(fingerprint *feature, int64_t id);

#endif /* FEATURE_INDEX_H_ */
