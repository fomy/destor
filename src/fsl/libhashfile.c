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

#define _FILE_OFFSET_BITS 64
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#include <time.h>

#include "libhashfile.h"

#define max2(a, b)	((a) > (b) ? (a) : (b))
#define max(a, b, c)	(max2(max2(a, b), c))
#define FILE_HEADER_SIZE	\
	max(sizeof(struct file_header), sizeof(struct file_header_v2), \
			sizeof(struct file_header_v3))

static void convert_to_abstract_file_header(int version, uint8_t *fhdr,
				struct abstract_file_header *header)
{
	struct file_header *fhdr1;
	struct file_header_v2 *fhdr2;
	struct file_header_v3 *fhdr4;
	struct file_header_v4 *fhdr5;

	switch (version) {
	case HASH_FILE_VERSION1:
		fhdr1 = (struct file_header *)fhdr;
		header->file_size = fhdr1->file_size;
		header->chunks = fhdr1->chunks;
		header->pathlen = strlen(fhdr1->path);
		strcpy(header->path, fhdr1->path);
		break;
	case HASH_FILE_VERSION2:
	case HASH_FILE_VERSION3:
		fhdr2 = (struct file_header_v2 *)fhdr;
		header->file_size = fhdr2->file_size;
		header->chunks = fhdr2->chunks;
		header->pathlen = fhdr2->pathlen;
		/* not copying path here */
		break;
	case HASH_FILE_VERSION4:
		fhdr4 = (struct file_header_v3 *)fhdr;
		header->file_size = fhdr4->file_size;
		header->uid = fhdr4->uid;
		header->gid = fhdr4->gid;
		header->perm = fhdr4->perm;
		header->atime = fhdr4->atime;
		header->mtime = fhdr4->mtime;
		header->ctime = fhdr4->ctime;
		header->hardlinks = fhdr4->hardlinks;
		header->deviceid = fhdr4->deviceid;
		header->inodenum = fhdr4->inodenum;
		header->chunks = fhdr4->chunks;
		header->pathlen = fhdr4->pathlen;
		header->target_pathlen = fhdr4->target_pathlen;
		/* not copying path here */
		break;
	case HASH_FILE_VERSION5:
	case HASH_FILE_VERSION6:
	case HASH_FILE_VERSION7:
		fhdr5 = (struct file_header_v4 *)fhdr;
		header->file_size = fhdr5->file_size;
		header->blocks = fhdr5->blocks;
		header->uid = fhdr5->uid;
		header->gid = fhdr5->gid;
		header->perm = fhdr5->perm;
		header->atime = fhdr5->atime;
		header->mtime = fhdr5->mtime;
		header->ctime = fhdr5->ctime;
		header->hardlinks = fhdr5->hardlinks;
		header->deviceid = fhdr5->deviceid;
		header->inodenum = fhdr5->inodenum;
		header->chunks = fhdr5->chunks;
		header->pathlen = fhdr5->pathlen;
		header->target_pathlen = fhdr5->target_pathlen;
		/* not copying path here */
		break;
	default:
		assert(0);
	}
}

static inline int version_supported(uint32_t version)
{
	if (version == HASH_FILE_VERSION1 ||
		version == HASH_FILE_VERSION2 ||
		version == HASH_FILE_VERSION3 ||
		version == HASH_FILE_VERSION4 ||
		version == HASH_FILE_VERSION5 ||
		version == HASH_FILE_VERSION6 ||
		version == HASH_FILE_VERSION7)
		return 1;
	return 0;
}

struct hashfile_handle *hashfile_open(char *hashfile_name)
{
	int fd;
	int ret;
	struct hashfile_handle *handle;
	int saved_errno = 0;

	handle = (struct hashfile_handle *)malloc(sizeof(*handle));
	if (!handle)
		goto out;

	fd = open(hashfile_name, O_RDONLY);
	if (fd < 0) {
		saved_errno = errno;
		goto free_handle;
	}

	/*
	 * Reading the smallest possible header here (v1 or v2).
	 * Additional fields are read later if needed (for newer versions).
	 * We enforce that new fields must be added in the end of the header.
	 */
	ret = read(fd, &handle->header, sizeof(struct header));
	if (ret != sizeof(struct header)) {
		if (ret >= 0)
			saved_errno = EAGAIN;
		else
			saved_errno = errno;
		goto close_file;
	}

	if (handle->header.magic != HASH_FILE_MAGIC) {
		saved_errno = EINVAL;
		goto close_file;
	}

	if (!version_supported(handle->header.version)) {
		saved_errno = ENOTSUP;
		goto close_file;
	}

	/*
	 * Check if it is version 5 or 3/4 (which have longer headers).
	 * If yes, then we need to read more.
	 * For simplicity, just re-read whole header again.
	 */
	if (handle->header.version >= HASH_FILE_VERSION5) {
		ret = lseek(fd, 0, SEEK_SET);
		if (ret == -1) {
			saved_errno = errno;
			goto close_file;
		}

		ret = read(fd, &handle->header, sizeof(struct header_v4));
		if (ret != sizeof(struct header_v4)) {
			if (ret >= 0)
				saved_errno = EAGAIN;
			else
				saved_errno = errno;
			goto close_file;
		}
	} else if (handle->header.version >= HASH_FILE_VERSION3) {
		ret = lseek(fd, 0, SEEK_SET);
		if (ret == -1) {
			saved_errno = errno;
			goto close_file;
		}

		ret = read(fd, &handle->header, sizeof(struct header_v3));
		if (ret != sizeof(struct header_v3)) {
			if (ret >= 0)
				saved_errno = EAGAIN;
			else
				saved_errno = errno;
			goto close_file;
		}
	}

	handle->omode = READ;
	handle->fd = fd;
	handle->num_files_processed = 0;
	handle->num_hashes_processed_current_file = 0;
	handle->current_file.chunks = 0;

	handle->current_chunk_info.hash =
			(uint8_t *)malloc(handle->header.hash_size / 8);
	if (!handle->current_chunk_info.hash) {
		saved_errno = errno;
		goto close_file;
	}

	return handle;

close_file:
	close(fd);
free_handle:
	free(handle);
	errno = saved_errno;
out:
	return NULL;
}

struct hashfile_handle *hashfile_open4write(char *hashfile_name, enum
			chnking_method cmeth, enum hshing_method hmeth,
			uint32_t hash_size, const char *root_path)
{
	int fd;
	int ret;
	struct hashfile_handle *handle;
	int saved_errno;
	struct utsname utsn;
	time_t curtime;

	handle = (struct hashfile_handle *)malloc(sizeof(*handle));
	if (!handle)
		goto out;

	handle->current_chunk_info.hash = (uint8_t *)malloc(hash_size / 8);
	if (!handle->current_chunk_info.hash) {
		saved_errno = errno;
		goto free_handle;
	}

	fd = open(hashfile_name, O_CREAT | O_EXCL | O_WRONLY,
			S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP
				| S_IROTH | S_IWOTH);
	if (fd < 0) {
		saved_errno = errno;
		goto free_hash;
	}

	/* setting handle fields */
	handle->omode = WRITE;
	handle->fd = fd;
	handle->num_files_processed = 0;
	handle->num_hashes_processed_current_file = 0;

	/* setting the header fields */
	handle->header.magic = HASH_FILE_MAGIC;
	handle->header.version = HASH_FILE_VERSION7;
	handle->header.chnk_method = cmeth;
	handle->header.hsh_method = hmeth;
	handle->header.hash_size = hash_size;
	handle->header.chunks = 0;
	handle->header.files = 0;
	handle->header.bytes = 0;

	/* saving the real root path */
	strncpy(handle->header.path_root, root_path,
			sizeof(handle->header.path_root));

	/* saving the system id */
	ret = uname(&utsn);
	if (ret < 0) {
		saved_errno = errno;
		goto close_file;
	}

	snprintf(handle->header.sysid, MAX_SYSID_LEN,
			 "%s %s %s %s", utsn.sysname, utsn.release,
					 utsn.machine, utsn.nodename);

	/* saving the start time */
	curtime = time(NULL);
	if (curtime == ((time_t)-1)) {
		saved_errno = errno;
		goto close_file;
	}

	handle->header.start_time = curtime;

	/*
	 * writing the main header. it will be overwritten in
	 * hashfile_close() to reflect the file/chunk counters.
	 */
	ret = write(handle->fd, &handle->header, sizeof(handle->header));
	if (ret != sizeof(handle->header)) {
		if (ret < 0)
			saved_errno = errno;
		else
			saved_errno = EAGAIN;

		goto close_file;
	}

	return handle;

close_file:
	close(fd);
free_hash:
	free(handle->current_chunk_info.hash);
free_handle:
	free(handle);
	errno = saved_errno;
out:
	return NULL;
}

int hashfile_set_fxd_chnking_params(struct hashfile_handle *handle,
					struct fixed_chnking_params *params)
{
	if (handle->omode != WRITE) {
		errno = EBADF;
		return -1;
	}

	if (handle->header.chnk_method != FIXED) {
		errno = EINVAL;
		return -1;
	}

	memcpy(&handle->header.chnk_method_params.fixed_params,
					 params, sizeof(*params));

	return 0;
}

int hashfile_set_var_chnking_params(struct hashfile_handle *handle,
						struct var_chnking_params *params)
{
	if (handle->omode != WRITE) {
		errno = EBADF;
		return -1;
	}

	if (handle->header.chnk_method != VARIABLE) {
		errno = EINVAL;
		return -1;
	}

	memcpy(&handle->header.chnk_method_params.var_params,
					 params, sizeof(*params));

	return 0;
}

uint32_t hashfile_version(struct hashfile_handle *handle)
{
	return handle->header.version;
}

const char *hashfile_rootpath(struct hashfile_handle *handle)
{
	return handle->header.path_root;
}

const char *hashfile_sysid(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION3)
		return handle->header.sysid;
	else
		return NULL;
}

uint64_t hashfile_start_time(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION3)
		return handle->header.start_time;
	else
		return 0;
}

uint64_t hashfile_end_time(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION3)
		return handle->header.end_time;
	else
		return 0;
}

uint64_t hashfile_numfiles(struct hashfile_handle *handle)
{
	return handle->header.files;
}

uint64_t hashfile_numchunks(struct hashfile_handle *handle)
{
	return handle->header.chunks;
}

uint32_t hashfile_hash_size(struct hashfile_handle *handle)
{
	return handle->header.hash_size;
}

uint64_t hashfile_numbytes(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION5)
		return handle->header.bytes;
	else
		return 0;
}

enum chnking_method hashfile_chunking_method(struct hashfile_handle *handle)
{
	return handle->header.chnk_method;
}

int hashfile_fxd_chunking_params(struct hashfile_handle *handle,
					struct fixed_chnking_params *params)
{
	if (handle->header.chnk_method != FIXED) {
		errno = EINVAL;
		return -1;
	}

	memcpy(params, &handle->header.chnk_method_params.fixed_params,
					  sizeof(*params));

	return 0;
}

int hashfile_var_chunking_params(struct hashfile_handle *handle,
						struct var_chnking_params *params)
{
	if (handle->header.chnk_method != VARIABLE) {
		errno = EINVAL;
		return -1;
	}

	memcpy(params, &handle->header.chnk_method_params.var_params,
					 sizeof(*params));

	return 0;
}

int hashfile_chunking_method_str(struct hashfile_handle *handle,
					char *buf, int size)
{
	switch (handle->header.chnk_method) {
	case FIXED:
		snprintf(buf, size, "Fixed-%d\n",
					handle->header.chnk_method_params.
					fixed_params.chunk_size);
		break;
	case VARIABLE:
		switch(handle->header.chnk_method_params.var_params.algo) {
		case RANDOM:
			snprintf(buf, size, "Variable-random, p=%Lf",
				handle->header.chnk_method_params.
				var_params.algo_params.rnd_params.probability);
			break;
		case SIMPLE_MATCH:
			snprintf(buf, size, "Variable-simple_match, bits=%d,"
				" pattern=%" PRIx64,
				handle->header.chnk_method_params.var_params.
				algo_params.simple_params.bits_to_compare,
				handle->header.chnk_method_params.var_params.
				algo_params.simple_params.pattern);
			break;
		case RABIN:
			snprintf(buf, size, "Variable-rabin bits=%d, "
				 "pattern=%" PRIx64 ", Window size=%d",
				handle->header.chnk_method_params.var_params.
				algo_params.rabin_params.bits_to_compare,
				handle->header.chnk_method_params.var_params.
				algo_params.rabin_params.pattern,
				handle->header.chnk_method_params.var_params.
				algo_params.rabin_params.window_size);
			break;
		default:
			errno = EINVAL;
			return -1;
		}

		snprintf(buf + strlen(buf), size - strlen(buf),
			 "[%d:%d]\n", handle->header.chnk_method_params.
				var_params.min_csize,
				handle->header.chnk_method_params.
				var_params.max_csize);
		break;
	default:
		errno = EINVAL;
		return -1;
	}

	return 0;
}

enum hshing_method hashfile_hashing_method(struct hashfile_handle *handle)
{
	return handle->header.hsh_method;
}

int hashfile_hashing_method_str(struct hashfile_handle *handle,
				char *buf, int size)
{
	switch(handle->header.hsh_method) {
	case MD5_HASH:
	case MD5_48BIT_HASH:
		snprintf(buf, size, "MD5-%d\n", handle->header.hash_size);
		break;
	case SHA256_HASH:
		snprintf(buf, size, "SHA256-%d\n", handle->header.hash_size);
		break;
	case SHA1_HASH:
		snprintf(buf, size, "SHA1-%d\n", handle->header.hash_size);
		break;
	case MURMUR_HASH:
		snprintf(buf, size, "MURMUR-%d\n", handle->header.hash_size);
		break;
	default:
		errno = EINVAL;
		return -1;
	}

	return 0;
}

static uint64_t skip_over_current_file_hashes(struct hashfile_handle *handle)
{
	uint64_t skip_chunk_count;
	uint64_t chunk_info_block_size;
	uint64_t ret;

	chunk_info_block_size = handle->header.hash_size / 8;

	if (handle->header.chnk_method == VARIABLE) {
		if (handle->header.version >= HASH_FILE_VERSION7)
			chunk_info_block_size += CHUNK_SIZE_32BIT;
		else if (handle->header.version >= HASH_FILE_VERSION3)
			chunk_info_block_size +=CHUNK_SIZE_64BIT;
	}

	if (handle->header.version >= HASH_FILE_VERSION6)
		chunk_info_block_size +=
			sizeof(handle->current_chunk_info.cratio);

	skip_chunk_count = handle->current_file.chunks -
				 handle->num_hashes_processed_current_file;

	ret = lseek(handle->fd,
			 skip_chunk_count * chunk_info_block_size,
			 SEEK_CUR);
	return ret;
}

/* This function gets the next file information from hash file into
 * current_file field of hashfile_handle. Prepares the abstract_file_header
 * for the same. The return values from this function are little different.
 * 0 indicates EOF, 1 indicates success and < 0 indicates error -- typical
 * cases of read system call.
 */
int hashfile_next_file(struct hashfile_handle *handle)
{
	uint8_t file_header[FILE_HEADER_SIZE];
	int size;
	int ret;
	uint64_t skip_ret;

	if (handle->omode != READ) {
		errno = EBADF;
		return -1;
	}

	/* EOF Condition */
	if (handle->num_files_processed == handle->header.files)
		return 0;

	assert(handle->num_files_processed < handle->header.files);

	/* Skip any remaining hashes of current file */
	skip_ret = skip_over_current_file_hashes(handle);
	if (skip_ret < 0)
		return -1;

	/* Setting this because in case user retries for EAGAIN error, the
	 * file pointer should not really move. We are doing it only when
	 * lseek is successful
	 */
	handle->num_hashes_processed_current_file = handle->current_file.chunks;

	if (handle->header.version == HASH_FILE_VERSION5 ||
			handle->header.version == HASH_FILE_VERSION6 ||
			handle->header.version == HASH_FILE_VERSION7)
		size = sizeof(struct file_header_v4);
	else if (handle->header.version == HASH_FILE_VERSION4)
		size = sizeof(struct file_header_v3);
	else if (handle->header.version >= HASH_FILE_VERSION2)
		size = sizeof(struct file_header_v2);
	else if (handle->header.version == HASH_FILE_VERSION1)
		size = sizeof(struct file_header);
	else {
		size = 0; /* to make compiler happy */
		assert(0);
	}

	ret = read(handle->fd, file_header, size);
	if (ret != size) {
		if (ret >= 0) {
			errno = EAGAIN;
			ret = -1;
		}
		goto out;
	}

	convert_to_abstract_file_header(handle->header.version,
					file_header, &handle->current_file);

	if (handle->header.version >= HASH_FILE_VERSION2) {
		ret = read(handle->fd, handle->current_file.path,
					handle->current_file.pathlen);
		if (ret != handle->current_file.pathlen) {
			if (ret >= 0) {
				errno = EAGAIN;
				ret = -1;
			}
			goto out;
		}
		handle->current_file.path[handle->current_file.pathlen] = '\0';

		if (handle->header.version >= HASH_FILE_VERSION4 &&
					S_ISLNK(handle->current_file.perm)) {
			ret = read(handle->fd, handle->current_file.target_path,
					handle->current_file.target_pathlen);
			if (ret != handle->current_file.target_pathlen) {
				if (ret >= 0) {
					errno = EAGAIN;
					ret = -1;
				}
				goto out;
			}
			handle->current_file.target_path
				[handle->current_file.target_pathlen] = '\0';
		}
	}

	handle->num_files_processed++;
	handle->num_hashes_processed_current_file = 0;
	ret = 1;

out:
	return ret;
}

static int do_add_file(struct hashfile_handle *handle, const char *file_path,
			const struct stat *stat_buf, const char *target_path,
				 int only_finilize)
{
	struct file_header_v4 fheader;
	off_t cur_offset;
	off_t offset_ret;
	int ret;

	/* finalize previous file */
	if (handle->num_files_processed > 0) {
		/* saving current position in the hash file */
		cur_offset = lseek(handle->fd, 0, SEEK_CUR);
		if (cur_offset == (off_t)-1)
			return -1;

		/* seeking to the previous file's header position */
		offset_ret = lseek(handle->fd,
			handle->current_file_header_offset, SEEK_SET);
		if (offset_ret == (off_t)-1)
			return -1;

		/* filling the real header from the abstract header
				information and chunk counters */
		fheader.file_size = handle->current_file.file_size;
		fheader.blocks = handle->current_file.blocks;
		fheader.uid = handle->current_file.uid;
		fheader.gid = handle->current_file.gid;
		fheader.perm = handle->current_file.perm;
		fheader.atime = handle->current_file.atime;
		fheader.mtime = handle->current_file.mtime;
		fheader.ctime = handle->current_file.ctime;
		fheader.hardlinks = handle->current_file.hardlinks;
		fheader.deviceid = handle->current_file.deviceid;
		fheader.inodenum = handle->current_file.inodenum;
		fheader.chunks = handle->num_hashes_processed_current_file;
		fheader.pathlen = handle->current_file.pathlen;
		fheader.target_pathlen = handle->current_file.target_pathlen;

		/* writing the real header */
		ret = write(handle->fd, &fheader, sizeof(fheader));
		if (ret != sizeof(fheader)) {
			if (ret >= 0)
				errno = EAGAIN;
			return -1;
		}

		/* writing the file path */
		ret = write(handle->fd, handle->current_file.path,
					handle->current_file.pathlen);
		if (ret != handle->current_file.pathlen) {
			if (ret >= 0)
				errno = EAGAIN;
			return -1;
		}

		/* writing target path in case of symlink */
		if (handle->current_file.target_pathlen) {
			ret = write(handle->fd,
					handle->current_file.target_path,
					handle->current_file.target_pathlen);
			if (ret != handle->current_file.target_pathlen) {
				if (ret >= 0)
					errno = EAGAIN;
				return -1;
			}
		}

		/* seeking back to new file's header position and saving it */
		handle->current_file_header_offset = lseek(handle->fd,
						cur_offset, SEEK_SET);
		if (handle->current_file_header_offset == (off_t)-1)
			return -1;
	}

	if (only_finilize)
		return 0;

	handle->num_files_processed++;
	handle->num_hashes_processed_current_file = 0;

	/* filling the abstract header, we'll need this data later */
	handle->current_file.file_size = stat_buf->st_size;
	handle->current_file.blocks = stat_buf->st_blocks;
	handle->current_file.uid = stat_buf->st_uid;
	handle->current_file.gid = stat_buf->st_gid;
	handle->current_file.perm = stat_buf->st_mode;
	handle->current_file.atime = stat_buf->st_atime;
	handle->current_file.mtime = stat_buf->st_mtime;
	handle->current_file.ctime = stat_buf->st_ctime;
	handle->current_file.hardlinks = stat_buf->st_nlink;
	handle->current_file.deviceid = stat_buf->st_dev;
	handle->current_file.inodenum = stat_buf->st_ino;
	handle->current_file.chunks = 0;
	handle->current_file.pathlen = strlen(file_path);
	strncpy(handle->current_file.path, file_path, MAX_PATH_SIZE);
	if (S_ISLNK(handle->current_file.perm)) {
		strncpy(handle->current_file.target_path,
			target_path, sizeof(handle->current_file.target_path));
		handle->current_file.target_pathlen =
			strlen(handle->current_file.target_path);
	} else {
		handle->current_file.target_pathlen = 0;
	}

	/* filling the most recent version header from the abstract one */
	fheader.file_size = handle->current_file.file_size;
	fheader.blocks = handle->current_file.blocks;
	fheader.uid = handle->current_file.uid;
	fheader.gid = handle->current_file.gid;
	fheader.perm = handle->current_file.perm;
	fheader.atime = handle->current_file.atime;
	fheader.mtime = handle->current_file.mtime;
	fheader.ctime = handle->current_file.ctime;
	fheader.hardlinks = handle->current_file.hardlinks;
	fheader.deviceid = handle->current_file.deviceid;
	fheader.inodenum = handle->current_file.inodenum;
	fheader.chunks = handle->current_file.chunks;
	fheader.pathlen = handle->current_file.pathlen;
	fheader.target_pathlen = handle->current_file.target_pathlen;

	/* saving current's file header offset */
	handle->current_file_header_offset = lseek(handle->fd, 0, SEEK_CUR);
	if (handle->current_file_header_offset == (off_t)-1)
		return -1;

	/*
	 * writing the real header. That's a preliminary write,
	 * the header will be overwritten later when
	 * the file is finalized.
	 */
	ret = write(handle->fd, &fheader, sizeof(fheader));
	if (ret != sizeof(fheader)) {
		if (ret >= 0)
			errno = EAGAIN;
		return -1;
	}

	/* writing the file path */
	ret = write(handle->fd, handle->current_file.path,
				handle->current_file.pathlen);
	if (ret != handle->current_file.pathlen) {
		if (ret >= 0)
			errno = EAGAIN;
		return -1;
	}

	/* writing target path in case of symlink */
	if (S_ISLNK(handle->current_file.perm)) {
		ret = write(handle->fd, handle->current_file.target_path,
					handle->current_file.target_pathlen);
		if (ret != handle->current_file.target_pathlen) {
			if (ret >= 0)
				errno = EAGAIN;
			return -1;
		}
	}

	return 0;
}

int hashfile_add_file(struct hashfile_handle *handle,
			const char *file_path, const struct stat *stat_buf,
				const char *target_path)
{
	if (handle->omode != WRITE) {
		errno = EBADF;
		return -1;
	}

	return do_add_file(handle, file_path, stat_buf, target_path, 0);
}

void hashfile_close(struct hashfile_handle *handle)
{
	time_t curtime;
	int ret;

	if (handle->omode == WRITE) {

		/* finalizing the last file in the hashfile */
		(void)do_add_file(handle, NULL, NULL, NULL, 1);

		/* saving the end time */
		curtime = time(NULL);
		if (curtime != ((time_t)-1))
			handle->header.end_time = curtime;

		/* handle->header.chunks is already updated as needed */
		handle->header.files = handle->num_files_processed;

		lseek(handle->fd, 0, SEEK_SET);
		ret = write(handle->fd, &handle->header,
					sizeof(handle->header));
		if (ret != sizeof(handle->header)) {
			/*
			 * something bad happened on write,
			 * but hashfile_close() function is void,
			 * so just ignore this error.
			 */
		}
	}

	free(handle->current_chunk_info.hash);
	close(handle->fd);
	free(handle);
}

const struct chunk_info *hashfile_next_chunk(struct hashfile_handle *handle)
{
	int ret;

	if (handle->omode != READ) {
		errno = EBADF;
		return NULL;
	}

	if (handle->current_file.chunks ==
			 handle->num_hashes_processed_current_file)
		return NULL;

	assert(handle->num_hashes_processed_current_file <
						 handle->current_file.chunks);

	if (handle->header.version >= HASH_FILE_VERSION7 &&
		handle->header.chnk_method == VARIABLE) {
		ret = read(handle->fd, &handle->current_chunk_info.size,
			 CHUNK_SIZE_32BIT);
		if (ret != CHUNK_SIZE_32BIT) {
			if (ret >= 0)
				errno = EAGAIN;
			return NULL;
		}
	} else if (handle->header.version >= HASH_FILE_VERSION3 &&
		handle->header.chnk_method == VARIABLE) {
		ret = read(handle->fd, &handle->current_chunk_info.size,
			 CHUNK_SIZE_64BIT);
		if (ret != CHUNK_SIZE_64BIT) {
			if (ret >= 0)
				errno = EAGAIN;
			return NULL;
		}
	} else if (handle->header.chnk_method == FIXED) {
		if (handle->current_file.chunks - 1 ==
			 handle->num_hashes_processed_current_file) {
				/* Last chunk */
				handle->current_chunk_info.size =
				handle->current_file.file_size -
				(handle->current_file.chunks - 1) *
				handle->header.chnk_method_params.fixed_params.chunk_size;
				/* Detect if tail was on or off */
				handle->current_chunk_info.size =
				(handle->current_chunk_info.size >
				handle->header.chnk_method_params.fixed_params.chunk_size)
				? handle->header.chnk_method_params.fixed_params.chunk_size :
				handle->current_chunk_info.size;
		} else {
			handle->current_chunk_info.size =
				handle->header.chnk_method_params.fixed_params.chunk_size;
		}
	} else {
		/*
		 * Hashfile version 2 does not have chunk size for
		 * variable chunking. So, just report 0.
		 */
		handle->current_chunk_info.size = 0;
	}

	ret = read(handle->fd, handle->current_chunk_info.hash,
			 handle->header.hash_size / 8);
	if (ret != handle->header.hash_size / 8) {
		if (ret >= 0)
			errno = EAGAIN;
		return NULL;
	}

	if (handle->header.version >= HASH_FILE_VERSION6) {
		ret = read(handle->fd, &handle->current_chunk_info.cratio,
			 sizeof(handle->current_chunk_info.cratio));
		if (ret != sizeof(handle->current_chunk_info.cratio)) {
			if (ret >= 0)
				errno = EAGAIN;
			return NULL;
		}
	} else {
		/*
		 * If cratio is not available (old hashfiles), set it to zero.
		 */
		handle->current_chunk_info.cratio = 0;
	}

	handle->num_hashes_processed_current_file++;
	return &handle->current_chunk_info;
}

int hashfile_add_chunk(struct hashfile_handle *handle,
				 const struct chunk_info *ci)
{
	int ret;

	if (handle->omode != WRITE) {
		errno = EBADF;
		return -1;
	}

	if (handle->header.chnk_method == VARIABLE) {
		ret = write(handle->fd, &ci->size, CHUNK_SIZE_32BIT);
		if (ret != CHUNK_SIZE_32BIT) {
			if (ret >= 0)
				errno = EAGAIN;
			return -1;
		}
	}

	ret = write(handle->fd, ci->hash, handle->header.hash_size / 8);
	if (ret != handle->header.hash_size / 8) {
		if (ret >= 0)
			errno = EAGAIN;
		return -1;
	}

	ret = write(handle->fd, &ci->cratio, sizeof(ci->cratio));
	if (ret != sizeof(ci->cratio)) {
		if (ret >= 0)
			errno = EAGAIN;
		return -1;
	}

	/* per-file chunk count */
	handle->num_hashes_processed_current_file++;
	/* global chunk count */
	handle->header.chunks++;
	/* global byte count */
	handle->header.bytes += ci->size;

	return 0;
}

const char *hashfile_curfile_path(struct hashfile_handle *handle)
{
	return handle->current_file.path;
}

uint64_t hashfile_curfile_numchunks(struct hashfile_handle *handle)
{
	return handle->current_file.chunks;
}

uint32_t hashfile_curfile_uid(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION4)
		return handle->current_file.uid;
	else
		return 0;
}

uint32_t hashfile_curfile_gid(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION4)
		return handle->current_file.gid;
	else
		return 0;
}

uint64_t hashfile_curfile_perm(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION4)
		return handle->current_file.perm;
	else
		return 0;
}

uint64_t hashfile_curfile_atime(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION4)
		return handle->current_file.atime;
	else
		return 0;
}

uint64_t hashfile_curfile_mtime(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION4)
		return handle->current_file.mtime;
	else
		return 0;
}

uint64_t hashfile_curfile_ctime(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION4)
		return handle->current_file.ctime;
	else
		return 0;
}

uint64_t hashfile_curfile_hardlinks(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION4)
		return handle->current_file.hardlinks;
	else
		return 0;
}

uint64_t hashfile_curfile_deviceid(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION4)
		return handle->current_file.deviceid;
	else
		return 0;
}

uint64_t hashfile_curfile_inodenum(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION4)
		return handle->current_file.inodenum;
	else
		return 0;
}

uint64_t hashfile_curfile_size(struct hashfile_handle *handle)
{
	return handle->current_file.file_size;
}

uint64_t hashfile_curfile_blocks(struct hashfile_handle *handle)
{
	return handle->current_file.blocks;
}

char *hashfile_curfile_linkpath(struct hashfile_handle *handle)
{
	if (handle->header.version >= HASH_FILE_VERSION4
				&& S_ISLNK(handle->current_file.perm)) {
		return handle->current_file.target_path;
	} else
		return NULL;
}

/* Function to restart the hash file scanning once the file is open. Typically
 * used when there are multiple scans of hash file are required
 * (e.g. bloomfilter). Useful, rather than opening and closing the file.
 */
int hashfile_reset(struct hashfile_handle *handle)
{
	int ret;

	if (handle->omode != READ) {
		errno = EBADF;
		return -1;
	}

	/* Skip over the initial global header */
	ret = lseek(handle->fd, sizeof(handle->header), SEEK_SET);
	if (ret == -1)
		return ret;

	handle->num_files_processed = 0;
	handle->num_hashes_processed_current_file = 0;
	handle->current_file.chunks = 0;

	return 0;
}
