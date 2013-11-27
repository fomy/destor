/*
 * global_fingerprit_index.h
 *
 *  Created on: Aug 25, 2013
 *      Author: fumin
 */

#ifndef GLOBAL_FINGERPRINT_INDEX_H_
#define GLOBAL_FINGERPRINT_INDEX_H_

#include "../destor.h"

/*
 *	A global fingerprint index that indexes all fingerprints in the system.
 *	It is implemented as a key-value database.
 *	The key is fingerprint,
 *	and the value is a pointer (either a container id or a segment id).
 *
 *	It can be considered as a special feature index.
 */

void db_init();
void db_close();
int64_t db_lookup_fingerprint(fingerprint *);
void db_insert_fingerprint(fingerprint*, int64_t);

#endif /* GLOBAL_FINGERPRINT_INDEX_H_ */
