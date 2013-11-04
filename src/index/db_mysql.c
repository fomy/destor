/*
 * db_mysql.c
 *
 *  Created on: Aug 25, 2013
 *      Author: fumin
 */
#include "../global.h"
#include <mysql/mysql.h>

static MYSQL *mdb = 0;

static char search_sql[] =
		"select ContainerId from HashStore where Fingerprint=? limit 1;";
static char search_sql_for_hint[] =
		"select BlockHint from HashStore where Fingerprint=? limit 1;";
static char insert_sql[] =
		"insert into HashStore(Fingerprint, ContainerId) values(?, ?) on duplicate key update ContainerId=?;";
static char insert_sql_with_hint[] =
		"insert into HashStore(Fingerprint, ContainerId, BlockHint) values(?, ?, ?) on duplicate key update ContainerId=?,BlockHint=?;";

static MYSQL_STMT *search_stmt;
static MYSQL_STMT *search_stmt_for_hint;
static MYSQL_STMT *insert_stmt;
static MYSQL_STMT *insert_stmt_with_hint;

static BOOL db_open_database() {
	mdb = mysql_init(NULL);
	if (!mysql_real_connect(mdb, "localhost", "destor", "123456", "destor_db",
			0, NULL, CLIENT_FOUND_ROWS)) {
		printf("%s, %d: failed to open database!\n", __FILE__, __LINE__);
		return FALSE;
	}
	return TRUE;
}

BOOL db_init() {
	if (!db_open_database()) {
		return FALSE;
	}
	search_stmt = mysql_stmt_init(mdb);
	insert_stmt = mysql_stmt_init(mdb);
	search_stmt_for_hint = mysql_stmt_init(mdb);
	insert_stmt_with_hint = mysql_stmt_init(mdb);
	mysql_stmt_prepare(search_stmt, search_sql, strlen(search_sql));
	mysql_stmt_prepare(insert_stmt, insert_sql, strlen(insert_sql));
	mysql_stmt_prepare(search_stmt_for_hint, search_sql_for_hint,
			strlen(search_sql_for_hint));
	mysql_stmt_prepare(insert_stmt_with_hint, insert_sql_with_hint,
			strlen(insert_sql_with_hint));
	return TRUE;
}

int64_t db_close() {
	int item_num = 0;
	int i = mysql_query(mdb, "select COUNT(Fingerprint) from HashStore;");
	if (i == 0) {
		MYSQL_RES *result = mysql_use_result(mdb);
		if (result) {
			MYSQL_ROW sqlrow = mysql_fetch_row(result);
			item_num = atoi(sqlrow[0]);
			/*printf("memory_overhead = %s\n", sqlrow[0]);*/
			mysql_free_result(result);
		}
	} else {
		dprint("Error");
	}

	mysql_stmt_close(search_stmt);
	mysql_stmt_close(insert_stmt);
	mysql_stmt_close(search_stmt_for_hint);
	mysql_stmt_close(insert_stmt_with_hint);
	mysql_close(mdb);
	return item_num;
}

ContainerId db_lookup_fingerprint(Fingerprint *finger) {
	unsigned long hashlen = sizeof(Fingerprint);
	MYSQL_BIND param[1];
	memset(param, 0, sizeof(param)); //Without this, excute and fetch will cause segmentation fault.
	param[0].buffer_type = MYSQL_TYPE_BLOB;
	param[0].buffer = finger;
	param[0].buffer_length = hashlen;
	param[0].length = &hashlen;

	if (mysql_stmt_bind_param(search_stmt, param) != 0) {
		printf("%s, %d: %s\n", __FILE__, __LINE__,
				mysql_stmt_error(search_stmt));
		return -1;
	}
	if (mysql_stmt_execute(search_stmt) != 0) {
		printf("%s, %d: failed to search index! %s\n", __FILE__, __LINE__,
				mysql_stmt_error(search_stmt));
		return -1;
	}

	ContainerId resultId = -1;
	unsigned long reslen = 0;
	MYSQL_BIND result[1];
	memset(result, 0, sizeof(result));
	my_bool is_null;
	result[0].buffer_type = MYSQL_TYPE_LONG;
	result[0].buffer = &resultId;
	result[0].length = &reslen;
	result[0].is_null = &is_null;

	if (mysql_stmt_bind_result(search_stmt, result)) {
		printf("%s, %d: %s\n", __FILE__, __LINE__,
				mysql_stmt_error(search_stmt));
		return -1;
	}
	if (mysql_stmt_store_result(search_stmt)) {
		printf("%s, %d: %s\n", __FILE__, __LINE__,
				mysql_stmt_error(search_stmt));
		return -1;
	}
	int ret = mysql_stmt_fetch(search_stmt);
	if (ret == MYSQL_NO_DATA) {
		/*printf("%s, %d: no such line.\n",__FILE__,__LINE__);*/
		return -1;
	} else if (ret == 1) {
		printf("%s, %d: %s\n", __FILE__, __LINE__,
				mysql_stmt_error(search_stmt));
		return -1;
	}
	mysql_stmt_free_result(search_stmt);
	return resultId;
}

int64_t db_lookup_fingerprint_for_hint(Fingerprint *finger) {
	unsigned long hashlen = sizeof(Fingerprint);
	MYSQL_BIND param[1];
	memset(param, 0, sizeof(param)); //Without this, excute and fetch will cause segmentation fault.
	param[0].buffer_type = MYSQL_TYPE_BLOB;
	param[0].buffer = finger;
	param[0].buffer_length = hashlen;
	param[0].length = &hashlen;

	if (mysql_stmt_bind_param(search_stmt_for_hint, param) != 0) {
		printf("%s, %d: %s\n", __FILE__, __LINE__,
				mysql_stmt_error(search_stmt_for_hint));
		return -1;
	}
	if (mysql_stmt_execute(search_stmt_for_hint) != 0) {
		printf("%s, %d: failed to search index! %s\n", __FILE__, __LINE__,
				mysql_stmt_error(search_stmt_for_hint));
		return -1;
	}

	int32_t block_hint = -1;
	unsigned long reslen = 0;
	MYSQL_BIND result[1];
	memset(result, 0, sizeof(result));
	my_bool is_null;
	result[0].buffer_type = MYSQL_TYPE_LONG;
	result[0].buffer = &block_hint;
	result[0].length = &reslen;
	result[0].is_null = &is_null;

	if (mysql_stmt_bind_result(search_stmt_for_hint, result)) {
		printf("%s, %d: %s\n", __FILE__, __LINE__,
				mysql_stmt_error(search_stmt_for_hint));
		return -1;
	}
	if (mysql_stmt_store_result(search_stmt_for_hint)) {
		printf("%s, %d: %s\n", __FILE__, __LINE__,
				mysql_stmt_error(search_stmt_for_hint));
		return -1;
	}
	int ret = mysql_stmt_fetch(search_stmt_for_hint);
	if (ret == MYSQL_NO_DATA) {
		/*printf("%s, %d: no such line.\n",__FILE__,__LINE__);*/
		return -1;
	} else if (ret == 1) {
		printf("%s, %d: %s\n", __FILE__, __LINE__,
				mysql_stmt_error(search_stmt_for_hint));
		return -1;
	}
	mysql_stmt_free_result(search_stmt_for_hint);
	return block_hint;
}

void db_insert_fingerprint(Fingerprint* finger, ContainerId id) {
	unsigned long hashlen = sizeof(Fingerprint);
	MYSQL_BIND param[3];
	memset(param, 0, sizeof(param));
	param[0].buffer_type = MYSQL_TYPE_BLOB;
	param[0].buffer = finger;
	param[0].buffer_length = hashlen;
	param[0].length = &hashlen;
	param[1].buffer_type = MYSQL_TYPE_LONG;
	param[1].buffer = &id;
	param[2].buffer_type = MYSQL_TYPE_LONG;
	param[2].buffer = &id;

	if (mysql_stmt_bind_param(insert_stmt, param)) {
		printf("%s, %d: failed to update index! %s\n", __FILE__, __LINE__,
				mysql_stmt_error(insert_stmt));
	}
	if (mysql_stmt_execute(insert_stmt) != 0) {
		printf("%s, %d: failed to update index! %s\n", __FILE__, __LINE__,
				mysql_stmt_error(insert_stmt));
	}
}

void db_insert_fingerprint_with_hint(Fingerprint* finger, ContainerId id,
		int64_t block_hint) {
	unsigned long hashlen = sizeof(Fingerprint);
	MYSQL_BIND param[5];
	memset(param, 0, sizeof(param));
	param[0].buffer_type = MYSQL_TYPE_BLOB;
	param[0].buffer = finger;
	param[0].buffer_length = hashlen;
	param[0].length = &hashlen;
	param[1].buffer_type = MYSQL_TYPE_LONG;
	param[1].buffer = &id;
	param[2].buffer_type = MYSQL_TYPE_LONG;
	param[2].buffer = &block_hint;
	param[3].buffer_type = MYSQL_TYPE_LONG;
	param[3].buffer = &id;
	param[4].buffer_type = MYSQL_TYPE_LONG;
	param[4].buffer = &block_hint;

	if (mysql_stmt_bind_param(insert_stmt_with_hint, param)) {
		printf("%s, %d: failed to update index! %s\n", __FILE__, __LINE__,
				mysql_stmt_error(insert_stmt_with_hint));
	}
	if (mysql_stmt_execute(insert_stmt_with_hint) != 0) {
		printf("%s, %d: failed to update index! %s\n", __FILE__, __LINE__,
				mysql_stmt_error(insert_stmt_with_hint));
	}
}
