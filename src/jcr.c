/*
 * jcr.cpp
 *
 *  Created on: Feb 15, 2012
 *      Author: fumin
 */

#include "jcr.h"
#include "recipe/recipemanage.h"

void init_backup_jcr(char *path) {

	jcr.path = sdsnew(path);
	struct stat s;
	if (stat(path, &s) != 0) {
		fprintf(stderr, "backup path does not exist!");
		exit(1);
	}
	if (S_ISDIR(s.st_mode) && jcr.path[sdslen(jcr.path) - 1] != '/')
		jcr.path = sdscat(jcr.path, "/");

	jcr.bv = create_backup_verion(jcr.path);

	jcr.id = jcr.bv->number;

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
	 * the time consuming of six dedup phase
	 */
	jcr.read_time = 0;
	jcr.chunk_time = 0;
	jcr.hash_time = 0;
	jcr.filter_time = 0;
	jcr.write_time = 0;
	/*	double test_time;*/

	jcr.read_chunk_time = 0;
	jcr.write_file_time = 0;
}

void init_restore_jcr(int revision, char *path) {
	jcr.path = sdsnew(path);

	if (jcr.path[sdslen(jcr.path) - 1] != '/')
		jcr.path = sdscat(jcr.path, "/");

	jcr.bv = open_backup_version(revision);

	jcr.id = revision;

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
	 * the time consuming of six dedup phase
	 */
	jcr.read_time = 0;
	jcr.chunk_time = 0;
	jcr.hash_time = 0;
	jcr.filter_time = 0;
	jcr.write_time = 0;
	/*	double test_time;*/

	jcr.read_chunk_time = 0;
	jcr.write_file_time = 0;
}
