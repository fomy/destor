/*
 * db_mysql.h
 *
 *  Created on: Aug 25, 2013
 *      Author: fumin
 */

#ifndef DB_MYSQL_H_
#define DB_MYSQL_H_

BOOL db_init();
int64_t db_close();
ContainerId db_lookup_fingerprint(Fingerprint *finger);
void db_insert_fingerprint(Fingerprint* finger, ContainerId id);
int64_t db_lookup_fingerprint_for_hint(Fingerprint *finger);
void db_insert_fingerprint_with_hint(Fingerprint* finger, ContainerId id,
		int64_t block_hint);

#endif /* DB_MYSQL_H_ */
