/*
 * sample_index.h
 *
 *  Created on: Jul 9, 2013
 *      Author: fumin
 */

#ifndef SAMPLE_INDEX_H_
#define SAMPLE_INDEX_H_

int sample_index_init();
void sample_index_destroy();
ContainerId sample_index_search(Fingerprint *fp);
void sample_index_update(Fingerprint* finger, ContainerId id);


#endif /* SAMPLE_INDEX_H_ */
