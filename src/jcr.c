/*
 * jcr.cpp
 *
 *  Created on: Feb 15, 2012
 *      Author: fumin
 */

#include "jcr.h"

struct jcr jcr;

void init_jcr(char *path) {
	jcr.path = sdsnew(path);

	struct stat s;
	if (stat(path, &s) != 0) {
		fprintf(stderr, "backup path does not exist!");
		exit(1);
	}
	if (S_ISDIR(s.st_mode) && jcr.path[sdslen(jcr.path) - 1] != '/')
		jcr.path = sdscat(jcr.path, "/");

	jcr.bv = NULL;

	jcr.id = TEMPORARY_ID;

    jcr.status = JCR_STATUS_INIT;

	jcr.file_num = 0;
	jcr.data_size = 0;
	jcr.unique_data_size = 0;
	jcr.chunk_num = 0;
	jcr.unique_chunk_num = 0;
	jcr.zero_chunk_num = 0;
	jcr.zero_chunk_size = 0;
	jcr.rewritten_chunk_num = 0;
	jcr.rewritten_chunk_size = 0;

	jcr.sparse_container_num = 0;
	jcr.inherited_sparse_num = 0;
	jcr.total_container_num = 0;

	jcr.total_time = 0;
	/*
	 * the time consuming of seven backup phase
	 */
	jcr.read_time = 0;
	jcr.chunk_time = 0;
	jcr.hash_time = 0;
	jcr.dedup_time = 0;
	jcr.rewrite_time = 0;
	jcr.filter_time = 0;
	jcr.write_time = 0;

	/*
	 * the time consuming of three restore phase
	 */
	jcr.read_recipe_time = 0;
	jcr.read_chunk_time = 0;
	jcr.write_chunk_time = 0;

	jcr.read_container_num = 0;
}

void init_backup_jcr(char *path) {

	init_jcr(path);

	jcr.bv = create_backup_version(jcr.path);

	jcr.id = jcr.bv->bv_num;
}

void init_restore_jcr(int revision, char *path) {

	init_jcr(path);

	jcr.bv = open_backup_version(revision);

	if(jcr.bv->deleted == 1){
		WARNING("The backup has been deleted!");
		exit(1);
	}

	jcr.id = revision;
}
