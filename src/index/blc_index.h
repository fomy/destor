/*
 * block_locality_index.h
 *
 *  Created on: Aug 24, 2013
 *      Author: fumin
 */
#ifndef BLC_H_
#define BLC_H_

int blc_index_init();
void blc_index_destroy();
ContainerId blc_index_search(Fingerprint *fp);
void blc_index_update(Fingerprint* finger, ContainerId id);

#endif



