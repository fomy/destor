/*
 * global_fingerprint_index.c
 *
 *  Created on: Aug 25, 2013
 *      Author: fumin
 */

#include <mysql/mysql.h>
#include "global_fingerprint_index.h"
#include "../tools/bloom_filter.h"
#include "../jcr.h"

static MYSQL *mdb = 0;

static char search_sql[] =
		"select Pointer from HashStore where Fingerprint=? limit 1;";
static char insert_sql[] =
		"insert into HashStore(Fingerprint, Pointer) values(?, ?) on duplicate key update Pointer=?;";

static MYSQL_STMT *search_stmt;
static MYSQL_STMT *insert_stmt;

static unsigned char* filter;

static void db_open_database() {
	mdb = mysql_init(NULL);
	if (!mysql_real_connect(mdb, "localhost", "destor", "123456", "destor_db",
			0, NULL, CLIENT_FOUND_ROWS)) {
		fprintf(stderr, "Failed to open database!\n");
		exit(1);
	}
}

void db_init() {
	db_open_database();

	search_stmt = mysql_stmt_init(mdb);
	insert_stmt = mysql_stmt_init(mdb);
	mysql_stmt_prepare(search_stmt, search_sql, strlen(search_sql));
	mysql_stmt_prepare(insert_stmt, insert_sql, strlen(insert_sql));

	filter = calloc(1, FILTER_SIZE_BYTES);

	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/bloom.filter");

	FILE *fp;
	if ((fp = fopen(indexpath, "r"))) {
		/* Exist */
		fread(filter, FILTER_SIZE_BYTES, 1, fp);
		fclose(fp);
	}

	sdsfree(indexpath);
}

int db_get_item_num() {
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
		fprintf(stderr, "Fail to get item num.\n");
		exit(1);
	}

	return item_num;
}

void db_close() {
	mysql_stmt_close(search_stmt);
	mysql_stmt_close(insert_stmt);
	mysql_close(mdb);

	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/bloom.filter");

	FILE *fp;
	if ((fp = fopen(indexpath, "w")) == NULL) {
		fprintf(stderr, "Can not open index/bloom.filter for write!");
		perror("The reason is");
		exit(1);
	}

	fwrite(filter, FILTER_SIZE_BYTES, 1, fp);
	fclose(fp);
	free(filter);

	sdsfree(indexpath);
}

int64_t db_lookup_fingerprint(fingerprint *fp) {
	/* search in bloom filter */
	if (!in_dict(filter, (char*) fp, sizeof(fingerprint))) {
		return TEMPORARY_ID;
	}

	jcr.index_lookup_io++;

	unsigned long hashlen = sizeof(fingerprint);
	MYSQL_BIND param[1];
	memset(param, 0, sizeof(param)); //Without this, excute and fetch will cause segmentation fault.
	param[0].buffer_type = MYSQL_TYPE_BLOB;
	param[0].buffer = fp;
	param[0].buffer_length = hashlen;
	param[0].length = &hashlen;

	if (mysql_stmt_bind_param(search_stmt, param) != 0) {
		WARNING("%s, %d: %s", __FILE__, __LINE__, mysql_stmt_error(search_stmt));
		return TEMPORARY_ID;
	}
	if (mysql_stmt_execute(search_stmt) != 0) {
		WARNING("%s, %d: failed to search index! %s", __FILE__, __LINE__,
				mysql_stmt_error(search_stmt));
		return TEMPORARY_ID;
	}

	int64_t resultId;
	unsigned long reslen = 0;
	MYSQL_BIND result[1];
	memset(result, 0, sizeof(result));
	my_bool is_null;
	result[0].buffer_type = MYSQL_TYPE_LONGLONG;
	result[0].buffer = &resultId;
	result[0].length = &reslen;
	result[0].is_null = &is_null;

	if (mysql_stmt_bind_result(search_stmt, result)) {
		WARNING("%s, %d: %s", __FILE__, __LINE__, mysql_stmt_error(search_stmt));
		return TEMPORARY_ID;
	}
	if (mysql_stmt_store_result(search_stmt)) {
		WARNING("%s, %d: %s", __FILE__, __LINE__, mysql_stmt_error(search_stmt));
		return TEMPORARY_ID;
	}
	int ret = mysql_stmt_fetch(search_stmt);
	if (ret == MYSQL_NO_DATA) {
		/*printf("%s, %d: no such line.\n",__FILE__,__LINE__);*/
		return TEMPORARY_ID;
	} else if (ret == 1) {
		WARNING("%s, %d: %s", __FILE__, __LINE__, mysql_stmt_error(search_stmt));
		return TEMPORARY_ID;
	}
	mysql_stmt_free_result(search_stmt);
	return resultId;
}

void db_insert_fingerprint(fingerprint* fp, int64_t id) {
	unsigned long hashlen = sizeof(fingerprint);
	MYSQL_BIND param[3];
	memset(param, 0, sizeof(param));
	param[0].buffer_type = MYSQL_TYPE_BLOB;
	param[0].buffer = fp;
	param[0].buffer_length = hashlen;
	param[0].length = &hashlen;
	param[1].buffer_type = MYSQL_TYPE_LONGLONG;
	param[1].buffer = &id;
	param[2].buffer_type = MYSQL_TYPE_LONGLONG;
	param[2].buffer = &id;

	if (mysql_stmt_bind_param(insert_stmt, param)) {
		WARNING("%s, %d: failed to update index! %s", __FILE__, __LINE__,
				mysql_stmt_error(insert_stmt));
	}
	if (mysql_stmt_execute(insert_stmt) != 0) {
		WARNING("%s, %d: failed to update index! %s", __FILE__, __LINE__,
				mysql_stmt_error(insert_stmt));
	}

	insert_word(filter, (char*) fp, sizeof(fingerprint));
}
