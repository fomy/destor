/**
 * @file ddfsindex.c
 * @Synopsis  Data Domain File System, in FAST'08.
 *  We use mysql for simple.
 * @author fumin, fumin@hust.edu.cn
 * @version 1
 * @date 2013-01-09
 */
#include "../global.h"
#include "../storage/container_cache.h"
#include "../tools/lru_cache.h"
#include "../tools/bloom_filter.h"
#include "db_mysql.h"

extern char working_path[];
extern int ddfs_cache_size;
extern int64_t index_memory_overhead;

extern int64_t index_read_entry_counter;
extern int64_t index_read_times;
extern int64_t index_write_entry_counter;
extern int64_t index_write_times;

static char indexpath[256];
static ContainerCache *fingers_cache;
static unsigned char* filter;

static BOOL dirty = FALSE;

/* interfaces */
BOOL ddfs_index_init() {
	if (db_init() == FALSE) {
		return FALSE;
	}

	fingers_cache = container_cache_new(ddfs_cache_size, FALSE, -1);

	/* read bloom filter */
	strcpy(indexpath, working_path);
	strcat(indexpath, "index/bloom_filter");
	int fd;
	if ((fd = open(indexpath, O_RDONLY | O_CREAT, S_IRWXU)) <= 0) {
		printf("Can not open index/bloom_filter!");
		return FALSE;
	}
	filter = malloc(FILTER_SIZE_BYTES);
	if (FILTER_SIZE_BYTES != read(fd, filter, FILTER_SIZE_BYTES)) {
		bzero(filter, FILTER_SIZE_BYTES);
	}
	close(fd);

	dirty = FALSE;
	return TRUE;
}

void ddfs_index_flush() {
	if (dirty == FALSE)
		return;
	/* flush bloom filter */
	int fd;
	if ((fd = open(indexpath, O_WRONLY | O_CREAT, S_IRWXU)) <= 0) {
		printf("Can not open index/bloom_filter!");
	}
	if (FILTER_SIZE_BYTES != write(fd, filter, FILTER_SIZE_BYTES)) {
		printf("%s, %d: Failed to flush bloom filter!\n", __FILE__, __LINE__);
	}
	close(fd);

	dirty = FALSE;
}

void ddfs_index_destroy() {

	index_memory_overhead = db_close(); // one byte for each chunk

	ddfs_index_flush();
	container_cache_free(fingers_cache);
	free(filter);
}

ContainerId ddfs_index_search(Fingerprint *finger) {
	ContainerId resultId = TMP_CONTAINER_ID;
	/* search in cache */
	Container *container = container_cache_lookup(fingers_cache, finger);
	if (container != 0) {
		return container->id;
	}

	/* search in bloom filter */
	if (!in_dict(filter, (char*) finger, sizeof(Fingerprint))) {
		return TMP_CONTAINER_ID;
	}

	/* search in database */
	resultId = db_lookup_fingerprint(finger);
	index_read_times++;

	if (resultId != TMP_CONTAINER_ID) {
		Container* container = container_cache_insert_container(fingers_cache,
				resultId);
		if (container) {
			index_read_times++;
			index_read_entry_counter += container_get_chunk_num(container);
		}
	}

	return resultId;
}

void ddfs_index_update(Fingerprint* finger, ContainerId id) {
	db_insert_fingerprint(finger, id);

	insert_word(filter, (char*) finger, sizeof(Fingerprint));
	dirty = TRUE;
	index_write_times++;
	index_write_entry_counter++;
}
