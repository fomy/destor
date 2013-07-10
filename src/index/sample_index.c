/*
 * sample_index.c
 *
 *  Created on: Jul 9, 2013
 *      Author: fumin
 */

#include "../global.h"
#include "../storage/container_cache.h"
#include "htable.h"

#define INDEX_ITEM_SIZE 24

extern int32_t sample_rate;

extern char working_path[];
extern int ddfs_cache_size;
extern int64_t index_memory_overhead;

static HTable *table;
static char indexpath[256];
static ContainerCache *fingers_cache;

/* interfaces */
BOOL sample_index_init() {

	fingers_cache = container_cache_new(ddfs_cache_size, FALSE, -1);

	/* read bloom filter */
	strcpy(indexpath, working_path);
	strcat(indexpath, "index/sample_index");
	int fd;
	if ((fd = open(indexpath, O_RDONLY | O_CREAT, S_IRWXU)) <= 0) {
		printf("Can not open index/sample_index!");
		return FALSE;
	}

	struct stat fileInfo;
	stat(indexpath, &fileInfo);
	size_t nFileSize = fileInfo.st_size;

	Fingerprint fp;
	ContainerId addr;
	if (nFileSize > 0) {
		int64_t itemNum;
		if (read(fd, &itemNum, 8) != 8) {
			puts("error!");
		}
		table = htable_new(itemNum);
		char buf[INDEX_ITEM_SIZE * 1024];
		while (itemNum) {
			int rlen = read(fd, buf, INDEX_ITEM_SIZE * 1024);
			int off = 0;
			while (off < rlen) {
				memcpy(&fp, buf + off, sizeof(Fingerprint));
				off += sizeof(Fingerprint);
				memcpy(&addr, buf + off, sizeof(ContainerId));
				off += sizeof(ContainerId);

				htable_insert(table, &fp, addr);
				--itemNum;
			}
		}
	} else {
		table = htable_new(1024 * 1024L);
	}
	close(fd);

	return TRUE;
}

void sample_index_flush() {
	int fd;
	if ((fd = open(indexpath, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU)) <= 0) {
		printf("Can not open index/sample_index!\n");
		return;
	}

	int64_t itemNum = htable_size(table);
	index_memory_overhead = itemNum * INDEX_ITEM_SIZE;
	write(fd, &itemNum, 8);

	char buf[INDEX_ITEM_SIZE * 1024];
	int len = 0;

	hlink* item = htable_first(table);
	while (item) {
		if (len == INDEX_ITEM_SIZE * 1024) {
			write(fd, buf, len);
			len = 0;
		}
		memcpy(buf + len, &item->key, sizeof(Fingerprint));
		len += sizeof(Fingerprint);
		memcpy(buf + len, &item->value, sizeof(ContainerId));
		len += sizeof(ContainerId);
		item = htable_next(table);
	}

	if (len > 0)
		write(fd, buf, len);

	close(fd);
}

void sample_index_destroy() {
	sample_index_flush();
	htable_destroy(table);
	container_cache_free(fingers_cache);
}

ContainerId sample_index_search(Fingerprint *fingerprint) {
	/* search in cache */
	Container *container = container_cache_lookup(fingers_cache, fingerprint);
	if (container != 0) {
		return container->id;
	}

	ContainerId* addr = htable_lookup(table, fingerprint);

	if (addr != NULL) {
		container_cache_insert_container(fingers_cache, *addr);
	}

	return addr == NULL ? TMP_CONTAINER_ID : *addr;
}

void sample_index_update(Fingerprint* finger, ContainerId id) {
	static int count = 0;
	if (count % sample_rate == 0) {
		htable_insert(table, finger, id);
		count = 0;
	}
	++count;
}
