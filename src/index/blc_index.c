/*
 * block_locality_index.c
 *
 *  Created on: Aug 24, 2013
 *      Author: fumin
 */

#include "../global.h"
#include "../dedup.h"
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
static LRUCache *block_cache;
static LRUCache *diff_cache;
static unsigned char* filter;

static int block_volume_fd;
static int block_size = 512 * (20 + 4) + 4; //each block contains 512 chunks, and a 4-byte head
static int64_t global_block_num;

typedef struct block {
	int64_t block_id;
	int32_t chunk_num;
	GHashTable *chunk_table;
} Block;

static Block* block_buffer = NULL;

Block* block_new(int64_t id) {
	Block* new_block = (Block*) malloc(sizeof(Block));
	new_block->block_id = id;
	new_block->chunk_num = 0;
	new_block->chunk_table = g_hash_table_new_full(g_int64_hash,
			g_fingerprint_equal, free, free);
	return new_block;
}

void block_free(Block* block) {
	g_hash_table_destroy(block->chunk_table);
	free(block);
}

static Block* read_block_from_volume(int64_t block_id) {
	if (block_id == global_block_num) {
		dprint("Warn! The block has not been written yet!");
		return NULL;
	}
	index_read_times++;

	Block* block = block_new(block_id);
	char buffer[block_size];
	lseek(block_volume_fd, block_id * block_size + 8, SEEK_SET);
	if (read(block_volume_fd, buffer, block_size) != block_size) {
		printf("%s, %d: %d\n", __FILE__, __LINE__, errno);
	}

	unser_declare;
	unser_begin(buffer, 0);
	int32_t chunk_num;
	unser_int32(chunk_num);

	index_read_entry_counter += chunk_num;

	block->chunk_num = chunk_num;
	while (chunk_num) {
		Fingerprint *fingerprint = (Fingerprint*) malloc(sizeof(Fingerprint));
		ContainerId *cid = (ContainerId*) malloc(sizeof(ContainerId));
		unser_bytes(fingerprint, sizeof(Fingerprint));
		unser_bytes(cid, sizeof(ContainerId));
		g_hash_table_insert(block->chunk_table, fingerprint, cid);
		--chunk_num;
	}
	return block;
}

static void append_block_to_volume(Block* block) {
	if (global_block_num != block->block_id) {
		dprint("error");
	}

	char buffer[block_size];
	ser_declare;
	ser_begin(buffer, 0);

	ser_int32(block->chunk_num);
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, block->chunk_table);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		ser_bytes(key, sizeof(Fingerprint));
		ser_bytes(value, sizeof(ContainerId));
	}

	lseek(block_volume_fd, block->block_id * block_size + 8, SEEK_SET);
	if (write(block_volume_fd, buffer, block_size) != block_size) {
		printf("%s, %d: errno=%d\n", __FILE__, __LINE__, errno);
	}
	global_block_num++;

	index_write_times++;
	index_write_entry_counter += block->chunk_num;
}

BOOL block_check_id(Block* block, int64_t *id) {
	return block->block_id == *id ? TRUE : FALSE;
}

BOOL block_contains(Block* block, Fingerprint *fingerprint) {
	return g_hash_table_contains(block->chunk_table, fingerprint) ? TRUE : FALSE;
}

/* interfaces */
BOOL blc_index_init() {
	if (db_init() == FALSE) {
		return FALSE;
	}

	block_cache = lru_cache_new(ddfs_cache_size);
	diff_cache = lru_cache_new(4);

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

	strcpy(indexpath, working_path);
	strcat(indexpath, "index/block_volume");
	if ((block_volume_fd = open(indexpath, O_RDWR | O_CREAT, S_IRWXU)) <= 0) {
		printf("Can not open index/block_volume!");
		return FALSE;
	}
	if (read(block_volume_fd, &global_block_num, 8) != 8) {
		global_block_num = 0;
		lseek(block_volume_fd, 0, SEEK_SET);
		write(block_volume_fd, &global_block_num, 8);
	}

	return TRUE;
}

void blc_index_flush() {

	/* flush bloom filter */
	strcpy(indexpath, working_path);
	strcat(indexpath, "index/bloom_filter");
	int fd;
	if ((fd = open(indexpath, O_WRONLY | O_CREAT, S_IRWXU)) <= 0) {
		printf("Can not open index/bloom_filter!");
	}
	if (FILTER_SIZE_BYTES != write(fd, filter, FILTER_SIZE_BYTES)) {
		printf("%s, %d: Failed to flush bloom filter!\n", __FILE__, __LINE__);
	}
	close(fd);

	if (block_buffer) {
		append_block_to_volume(block_buffer);
		block_free(block_buffer);
		block_buffer = NULL;
	}

	lseek(block_volume_fd, 0, SEEK_SET);
	write(block_volume_fd, &global_block_num, 8);
	close(block_volume_fd);

}

void blc_index_destroy() {
	index_memory_overhead = db_close(); // one byte for each chunk

	blc_index_flush();
	lru_cache_free(block_cache, block_free);
	lru_cache_free(diff_cache, free);
	free(filter);
}

static BOOL load_block_by_diff(int64_t* diff, Fingerprint* fingerprint) {
	int64_t block_id = global_block_num - *diff;
	if (lru_cache_contains(block_cache, block_check_id, &block_id) == TRUE)
		return FALSE;
	Block *block = read_block_from_volume(block_id);
	Block *evictor = lru_cache_insert(block_cache, block);
	if (evictor)
		block_free(evictor);

	if (g_hash_table_contains(block->chunk_table, fingerprint) == TRUE) {
		return TRUE;
	} else
		return FALSE;
}

ContainerId blc_index_search(Fingerprint *fingerprint) {

	/* search in cache */
	Block* block = lru_cache_lookup(block_cache, block_contains, fingerprint);
	if (block != NULL) {
		ContainerId *res = g_hash_table_lookup(block->chunk_table, fingerprint);
		if (res == NULL)
			dprint("error");
		return *res;
	}

	/* search in bloom filter */
	if (!in_dict(filter, (char*) fingerprint, sizeof(Fingerprint))) {
		return TMP_CONTAINER_ID;
	}

	/* According to diff cache, we load some blocks into block cache. */
	lru_cache_foreach_conditionally(diff_cache, load_block_by_diff,
			fingerprint);
	/* if successfully, the block will be the head of block_cache. */
	block = lru_cache_get_top(block_cache);
	if (block) {
		ContainerId *res = g_hash_table_lookup(block->chunk_table, fingerprint);
		if (res)
			return *res;
	}

	ContainerId resultId = TMP_CONTAINER_ID;
	/* search in database */
	int64_t block_hint = db_lookup_fingerprint_for_hint(fingerprint);

	if (block_hint >= 0) {
		Block* block = read_block_from_volume(block_hint);
		if (block) {
			index_read_times++;
			ContainerId* res = g_hash_table_lookup(block->chunk_table,
					fingerprint);
			if (res) {
				resultId = *res;
			} else {
				dprint("error");
				return TMP_CONTAINER_ID;
			}
			Block* evict_block = lru_cache_insert(block_cache, block);
			if (evict_block)
				block_free(evict_block);

			int64_t* current_diff = (int64_t*) malloc(sizeof(int64_t));
			*current_diff = global_block_num - block_hint;
			int64_t* evict_diff = lru_cache_insert(diff_cache, current_diff);
			if (evict_diff)
				free(evict_diff);
		} else {
			ContainerId* res = g_hash_table_lookup(block_buffer->chunk_table,
					fingerprint);
			if (res) {
				resultId = *res;
			} else {
				dprint("error!");
				return TMP_CONTAINER_ID;
			}
		}
	}

	return resultId;
}

void blc_index_update(Fingerprint* fingerprint, ContainerId id) {
	if (block_buffer == NULL)
		block_buffer = block_new(global_block_num);

	if (block_buffer->chunk_num == 512) {
		/* buffer is full */
		append_block_to_volume(block_buffer);
		block_free(block_buffer);
		block_buffer = block_new(global_block_num);
	}

	insert_word(filter, (char*) fingerprint, sizeof(Fingerprint));

	Fingerprint* new_fp = (Fingerprint*) malloc(sizeof(Fingerprint));
	memcpy(new_fp, fingerprint, sizeof(Fingerprint));
	ContainerId* new_cid = (ContainerId*) malloc(sizeof(ContainerId));
	*new_cid = id;

	g_hash_table_insert(block_buffer->chunk_table, new_fp, new_cid);
	block_buffer->chunk_num++;

	db_insert_fingerprint_with_hint(fingerprint, id, block_buffer->block_id);
	index_write_times++;
}
