/*
 * Copyright (c) 2011-2014 Vasily Tarasov
 * Copyright (c) 2011	   Will Buik
 * Copyright (c) 2011-2014 Philip Shilane
 * Copyright (c) 2011-2014 Erez Zadok
 * Copyright (c) 2011-2014 Geoff Kuenning
 * Copyright (c) 2011-2014 Stony Brook University
 * Copyright (c) 2011-2014 Harvey Mudd College
 * Copyright (c) 2011-2014 The Research Foundation of the State University of New York
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 3 as
 * published by the Free Software Foundation.
 */

#ifndef _HASHFILELIB_H
#define _HASHFILELIB_H
/*
 * Format description for index files
 */

/*
 * v.1:
 *
 * HEADER (struct header)
 * FILE_HEADER 1 (struct file_header)
 * <hash1><hash2>...<hashN_1>
 * FILE_HEADER 2 (struct file_header)
 * <hash1><hash2>...<hashN_2>
 * ...
 * FILE_HEADER M (struct file_header)
 * <hash1><hash2>...<hashN_M>
 *
 * v.2:
 *
 * Same as v.1, but file header
 * contains variable file name field
 * instead of a fixed one (struct file_header_v2).
 *
 * v.3:
 *
 * In the beginning:
 * HEADER (struct header_v3)
 * FILE_HEADER 1 (struct file_header_v2)
 *
 * Then, if FIXED size chunking:
 * <hash1><hash2>...<hashN_1>
 * FILE_HEADER 2 (struct file_header_v2)
 * <hash1><hash2>...<hashN_2>
 * ...
 * FILE_HEADER M (struct file_header_v2)
 * <hash1><hash2>...<hashN_M>
 *
 * And if VARIABLE size chunking (chunk size added):
 * <hash1><hash2>...<hashN_1>
 * FILE_HEADER 2 (struct file_header_v2)
 * <chunk_size1><hash1><chunk_size2><hash2>...<chunk_sizeN_2><hashN_2>
 * ...
 * FILE_HEADER M (struct file_header_v2)
 * <chunk_size1><hash1><chunk_size2><hash2>...<chunk_sizeN_2><hashN_M>
 *
 * chunk_size is uint64_t
 *
 * v.4:
 * Same as v.3, but FILE_HEADER is 'struct file_header_v3' now,
 * which contains a lot of information returned by stat() call.
 * Also, if a file is a symlink, then the file's path to which
 * this symlink points to is stored in the file header.
 *
 * v.5:
 * Same as v.4, but
 * HEADER is 'struct header_v4' (contains the number of bytes scanned)
 * FILE_HEADER is 'struct file_header_v4' (contains the number of 512B blocks
 * allocated by file systems).
 *
 * v.6:
 * Same as v.5, but compression ratio for each chunk is saved.
 * As a result, the whole structure looks like that:
 *
 * In the beginning:
 * HEADER (struct header_v4)
 *
 * Then, if FIXED size chunking:
 * FILE_HEADER 1 (struct file_header_v4)
 * <hash1><cratio1><hash2><cratio2>...<hashN_1><cratioN_1>
 * FILE_HEADER 2 (struct file_header_v4)
 * <hash1><cratio1><hash2><cratio2>...<hashN_1><cratioN_2>
 * ...
 * FILE_HEADER M (struct file_header_v4)
 * <hash1><cratio1><hash2><cratio2>...<hashN_M><cratioN_M>
 *
 * And if VARIABLE size chunking (chunk size added):
 * FILE_HEADER 1 (struct file_header_v4)
 * <chunk_size1><hash1><cratio1><chunk_size2><hash2><cratio2>...<chunk_sizeN_1><hashN_1><cratioN_1>

 * FILE_HEADER 2 (struct file_header_v4)
 * <chunk_size1><hash1><cratio1><chunk_size2><hash2><cratio2>...<chunk_sizeN_2><hashN_1><cratioN_2>
 * ...
 * FILE_HEADER M (struct file_header_v4)
 * <chunk_size1><hash1><cratio1><chunk_size2><hash2><cratio2>...<chunk_sizeN_M><hashN_M><cratioN_M>
 *
 * chunk_size is uint64_t
 * cratio is uint8_t
 *
 * v.7:
 * Same as v.7, but chunk_size is uint32_t. Notice, that this impacts
 * only the file format; chunk_info structure still holds a chunk_size
 * field of uint64_t type for compatibility with older versions.
 * As a result, the whole structure looks like that:
 *
 * In the beginning:
 * HEADER (struct header_v4)
 *
 * Then, if FIXED size chunking:
 * FILE_HEADER 1 (struct file_header_v4)
 * <hash1><cratio1><hash2><cratio2>...<hashN_1><cratioN_1>
 * FILE_HEADER 2 (struct file_header_v4)
 * <hash1><cratio1><hash2><cratio2>...<hashN_1><cratioN_2>
 * ...
 * FILE_HEADER M (struct file_header_v4)
 * <hash1><cratio1><hash2><cratio2>...<hashN_M><cratioN_M>
 *
 * And if VARIABLE size chunking (chunk size added):
 * FILE_HEADER 1 (struct file_header_v4)
 * <chunk_size1><hash1><cratio1><chunk_size2><hash2><cratio2>...<chunk_sizeN_1><hashN_1><cratioN_1>

 * FILE_HEADER 2 (struct file_header_v4)
 * <chunk_size1><hash1><cratio1><chunk_size2><hash2><cratio2>...<chunk_sizeN_2><hashN_1><cratioN_2>
 * ...
 * FILE_HEADER M (struct file_header_v4)
 * <chunk_size1><hash1><cratio1><chunk_size2><hash2><cratio2>...<chunk_sizeN_M><hashN_M><cratioN_M>
 *
 * chunk_size is uint32_t
 * cratio is uint8_t
 *
 */

#include <stdint.h>
#include <limits.h>
#include <sys/stat.h>

#define HASH_FILE_MAGIC 0xDEADDEAD
#define MAX_PATH_SIZE	4096
#define MAX_SYSID_LEN	4096

/*
 * We enforce that version values
 * should increase monotonically.
 */
#define HASH_FILE_VERSION1	0x1
#define HASH_FILE_VERSION2	0x2
#define HASH_FILE_VERSION3	0x3
#define HASH_FILE_VERSION4	0x4
#define HASH_FILE_VERSION5	0x5
#define HASH_FILE_VERSION6	0x6
#define HASH_FILE_VERSION7	0x7

enum chnking_method
{
	FIXED = 1,
	VARIABLE = 2
};

enum hshing_method
{
	MD5_HASH = 1,
	SHA256_HASH = 2,
	MD5_48BIT_HASH = 3,
	MURMUR_HASH = 4,
	MD5_64BIT_HASH = 5,
	SHA1_HASH = 6,
};

enum cmpr_method
{
	NONE = 0,
	ZLIB_DEF = 1
};

enum var_chnking_algo
{
	RANDOM = 1,
	SIMPLE_MATCH = 2,
	RABIN = 3
};

struct var_random_chnk_params {
	long double probability;	/* probability to chunk a stream */
} __attribute__((packed));

struct var_simple_chnk_params {
	uint32_t bits_to_compare;
	uint64_t pattern;
} __attribute__((packed));

struct var_rabin_chnk_params {
	uint32_t window_size; 		/* in bytes */
	uint64_t prime;
	uint64_t module;
	uint32_t bits_to_compare;
	uint64_t pattern;
} __attribute__((packed));

struct fixed_chnking_params
{
	uint32_t chunk_size;
} __attribute__((packed));

struct var_chnking_params
{
	enum var_chnking_algo algo;
	union {
		struct var_random_chnk_params rnd_params;
		struct var_simple_chnk_params simple_params;
		struct var_rabin_chnk_params rabin_params;
	} algo_params;
	uint32_t min_csize;		/* in bytes */
	uint32_t max_csize;		/* in bytes */
} __attribute__((packed));

/*
 * Header for version 1 and 2:
 *
 * We enforce that new fields should be added
 * in the end of a header.
 */
struct header {
	uint32_t magic;
	uint32_t version;
	uint64_t files;
	char path_root[MAX_PATH_SIZE];	/* where the scan has started */
	uint64_t chunks; 		/* number of chunks/indexes */
	enum chnking_method chnk_method;
	union {
		struct fixed_chnking_params fixed_params;
		struct var_chnking_params var_params;
	} chnk_method_params;
	enum hshing_method hsh_method;
	uint32_t hash_size;		/* in bits */
} __attribute__((packed));

/*
 * Header for version 3 and 4:
 *
 * - sisid field added
 * - start/end time added
 */
struct header_v3 {
	uint32_t magic;
	uint32_t version;
	uint64_t files;
	char path_root[MAX_PATH_SIZE];	/* where the scan has started */
	uint64_t chunks; 		/* number of chunks/indexes */
	enum chnking_method chnk_method;
	union {
		struct fixed_chnking_params fixed_params;
		struct var_chnking_params var_params;
	} chnk_method_params;
	enum hshing_method hsh_method;
	uint32_t hash_size;		/* in bits */
	char sysid[MAX_SYSID_LEN];
	uint64_t start_time;
	uint64_t end_time;
} __attribute__((packed));

/*
 * Header for version 5
 *
 * bytes field added
 *
 */
struct header_v4 {
	uint32_t magic;
	uint32_t version;
	uint64_t files;
	char path_root[MAX_PATH_SIZE];	/* where the scan has started */
	uint64_t chunks; 		/* number of chunks/indexes */
	enum chnking_method chnk_method;
	union {
		struct fixed_chnking_params fixed_params;
		struct var_chnking_params var_params;
	} chnk_method_params;
	enum hshing_method hsh_method;
	uint32_t hash_size;		/* in bits */
	char sysid[MAX_SYSID_LEN];
	uint64_t start_time;
	uint64_t end_time;
	uint64_t bytes;
} __attribute__((packed));

struct file_header {
	char path[MAX_PATH_SIZE];
	uint64_t file_size;		/* in bytes */
	uint64_t chunks;		/* chunks/indexes in this file */
} __attribute__((packed));

struct file_header_v2 {
	uint64_t file_size;		/* in bytes */
	uint64_t chunks;		/* chunks/indexes in this file */
	uint32_t pathlen;		/* length of the following path */
	char path[0];			/* non-null terminated path
								 of pathlen */
} __attribute__((packed));

struct file_header_v3 {
	uint64_t file_size;		/* in bytes */
	uint32_t uid;			/* uid of file owner */
	uint32_t gid;			/* gid of file owner */
	uint64_t perm;			/* file mode */
	uint64_t atime;			/* file atime */
	uint64_t mtime;			/* file mtime */
	uint64_t ctime;			/* file ctime */
	uint64_t hardlinks;		/* number of hardlinks */
	uint64_t deviceid;		/* file device id */
	uint64_t inodenum;		/* file inode number */
	uint64_t chunks;		/* chunks/indexes in this file */
	uint32_t pathlen;		/* length of the following path */
	uint32_t target_pathlen;	/* length of the following target_path */
	char path[0];			/* non-null terminated path
								of pathlen */
	char target_path[0];		/* non-null terminated
							 path of pathlen */
} __attribute__((packed));

struct file_header_v4 {
	uint64_t file_size;		/* in bytes */
	uint64_t blocks;		/* 512B blocks allocated by fs */
	uint32_t uid;			/* uid of file owner */
	uint32_t gid;			/* gid of file owner */
	uint64_t perm;			/* file mode */
	uint64_t atime;			/* file atime */
	uint64_t mtime;			/* file mtime */
	uint64_t ctime;			/* file ctime */
	uint64_t hardlinks;		/* number of hardlinks */
	uint64_t deviceid;		/* file device id */
	uint64_t inodenum;		/* file inode number */
	uint64_t chunks;		/* chunks/indexes in this file */
	uint32_t pathlen;		/* length of the following path */
	uint32_t target_pathlen;	/* length of the following target_path */
	char path[0];			/* non-null terminated path
								of pathlen */
	char target_path[0];		/* non-null terminated
							 path of pathlen */
} __attribute__((packed));

struct abstract_file_header {
	uint64_t file_size;
	uint64_t blocks;
	uint32_t uid;
	uint32_t gid;
	uint64_t perm;
	uint64_t atime;
	uint64_t mtime;
	uint64_t ctime;
	uint64_t hardlinks;
	uint64_t deviceid;
	uint64_t inodenum;
	uint64_t chunks;
	uint32_t pathlen;
	char path[MAX_PATH_SIZE];
	uint32_t target_pathlen;
	char target_path[MAX_PATH_SIZE];
};

struct chunk_info {
	uint8_t *hash; /* this points to static memory! */
	uint64_t size;
	uint8_t cratio;
};

#define CHUNK_SIZE_32BIT (sizeof(uint32_t))
#define CHUNK_SIZE_64BIT (sizeof(uint64_t))

enum openmode {
	READ = 0,
	WRITE = 1,
};

struct hashfile_handle {
	int 				fd;
	enum openmode			omode;
	struct header_v4		header;
	struct abstract_file_header 	current_file;
	/* offset of current file's header. Used only when we write a file. */
	off_t				current_file_header_offset;
	struct chunk_info		current_chunk_info;
	uint64_t			num_files_processed;
	uint64_t			num_hashes_processed_current_file;
};

struct hashfile_handle *hashfile_open(char *hashfile_name);
struct hashfile_handle *hashfile_open4write(char *hashfile_name,
		enum chnking_method cmeth, enum hshing_method hmeth,
		uint32_t hash_size, const char *root_path);
int hashfile_set_fxd_chnking_params(struct hashfile_handle *handle,
					struct fixed_chnking_params *params);
int hashfile_set_var_chnking_params(struct hashfile_handle *handle,
					struct var_chnking_params *params);
void hashfile_close(struct hashfile_handle *handle);
uint32_t hashfile_version(struct hashfile_handle *handle);
const char *hashfile_rootpath(struct hashfile_handle *handle);
/*
 * hashfile_sysid(), hashfile_start_time(), hashfile_end_time(),
 * hashfile_curfile_uid(), hashfile_curfile_gid(), and others
 * return NULL (or 0) if hashfile version does not support
 * corresponding header fields.
 */
const char *hashfile_sysid(struct hashfile_handle *handle);
uint64_t hashfile_start_time(struct hashfile_handle *handle);
uint64_t hashfile_end_time(struct hashfile_handle *handle);
uint64_t hashfile_numfiles(struct hashfile_handle *handle);
uint64_t hashfile_numchunks(struct hashfile_handle *handle);
uint64_t hashfile_numbytes(struct hashfile_handle *handle);
uint32_t hashfile_hash_size(struct hashfile_handle *handle);
enum chnking_method hashfile_chunking_method(struct hashfile_handle *handle);
int hashfile_chunking_method_str(struct hashfile_handle *handle,
				 char *buf, int size);
int hashfile_fxd_chunking_params(struct hashfile_handle *handle,
					struct fixed_chnking_params *params);
int hashfile_var_chunking_params(struct hashfile_handle *handle,
					struct var_chnking_params *params);
enum hshing_method hashfile_hashing_method(struct hashfile_handle *handle);
int hashfile_hashing_method_str(struct hashfile_handle *handle,
				 char *buf, int size);
int hashfile_next_file(struct hashfile_handle *handle);
int hashfile_add_file(struct hashfile_handle *handle, const char *file_path,
		const struct stat *stat_buf, const char *target_path);
const struct chunk_info *hashfile_next_chunk(struct hashfile_handle *handle);
int hashfile_add_chunk(struct hashfile_handle *handle,
				 const struct chunk_info *ci);
const char *hashfile_curfile_path(struct hashfile_handle *handle);
uint64_t hashfile_curfile_numchunks(struct hashfile_handle *handle);
uint32_t hashfile_curfile_uid(struct hashfile_handle *handle);
uint32_t hashfile_curfile_gid(struct hashfile_handle *handle);
uint64_t hashfile_curfile_perm(struct hashfile_handle *handle);
uint64_t hashfile_curfile_atime(struct hashfile_handle *handle);
uint64_t hashfile_curfile_mtime(struct hashfile_handle *handle);
uint64_t hashfile_curfile_ctime(struct hashfile_handle *handle);
uint64_t hashfile_curfile_size(struct hashfile_handle *handle);
uint64_t hashfile_curfile_hardlinks(struct hashfile_handle *handle);
uint64_t hashfile_curfile_deviceid(struct hashfile_handle *handle);
uint64_t hashfile_curfile_inodenum(struct hashfile_handle *handle);
char *hashfile_curfile_linkpath(struct hashfile_handle *handle);
uint64_t hashfile_curfile_blocks(struct hashfile_handle *handle);
int hashfile_reset(struct hashfile_handle *handle);

#endif /*_HASHFILELIB_H */
