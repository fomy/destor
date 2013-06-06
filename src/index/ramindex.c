#include "../global.h"
#include "../dedup.h"
#include "htable.h"

#define INDEX_ITEM_SIZE 24

extern char working_path[];

static char indexpath[256];
/* hash table */
static HTable *table;

static BOOL dirty = FALSE;

int ram_index_init() {
	strcpy(indexpath, working_path);
	strcat(indexpath, "index/ramindex.db");
	int fd;
	if ((fd = open(indexpath, O_RDONLY | O_CREAT, S_IRWXU)) <= 0) {
		dprint("Can not open index/ramindex.db!");
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

	dirty = FALSE;
	return TRUE;
}

void ram_index_flush() {
	if (dirty == FALSE)
		return;
	int fd;
	if ((fd = open(indexpath, O_WRONLY | O_CREAT, S_IRWXU)) <= 0) {
		printf("Can not open RAMDedup/hash.db!\n");
		return;
	}

	int64_t itemNum = htable_size(table);
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
	dirty = FALSE;
}

void ram_index_destroy() {
	ram_index_flush();
	htable_destroy(table);
}

ContainerId ram_index_search(Fingerprint *fp) {
	ContainerId* addr = htable_lookup(table, fp);
	return addr == NULL ? TMP_CONTAINER_ID : *addr;
}

void ram_index_update(Fingerprint* finger, ContainerId id) {
	htable_insert(table, finger, id);
	dirty = TRUE;
}
