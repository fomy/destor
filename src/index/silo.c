/**
 * @file silo.c
 * @Synopsis SiLo, in ATC'11.
 * SHTable, stored in ${working_path}/index/shtable
 * SiloBlock, stored in ${working_path}/index/block.vol,
 *      block_num, block1, block2, block3, ... (just append),
 * which are indexed by block id.
 *
 * The only difference is without segment-level dedup.
 * @author fumin, fumin@hust.edu.cn
 * @version 1
 * @date 2013-01-06
 */
#include "silo.h"
#include "../jcr.h"
#include "../tools/lru_cache.h"

extern int32_t silo_segment_size; //KB
extern int32_t silo_block_size; //MB
extern int32_t silo_read_cache_size;

extern uint32_t chunk_size;
extern char working_path[];
/* silo_x_size/average chunk size */
int32_t silo_segment_hash_size;
int32_t silo_block_hash_size;

int32_t silo_item_size = sizeof(Fingerprint) + sizeof(ContainerId);

#define BLOCK_HEAD_SIZE 8
/* segment representative fingerprint-block id mapping */
static GHashTable *SHTable;

static SiloBlock *write_buffer = NULL;
/* now we only maintain one block-sized cache */
static LRUCache *read_cache = NULL;

static int32_t block_num;
static int block_vol_fd;

static Fingerprint representative_fingerprint;

static SiloBlock* silo_block_new() {
	SiloBlock* block = (SiloBlock*) malloc(sizeof(SiloBlock));
	block->LHTable = htable_new(silo_block_size * 1024L * 1024 / chunk_size);
	block->id = -1;
	block->representative_table = g_hash_table_new(g_int64_hash,
			g_fingerprint_equal);
	block->size = 0;
	block->dirty = FALSE;

	return block;
}

static void silo_block_free(SiloBlock* block) {
	htable_destroy(block->LHTable);
	g_hash_table_destroy(block->representative_table);

	free(block);
}

/* id,num, finger-id */
static void append_block_to_volume(SiloBlock *block) {
	block->id = block_num;
	char buffer[silo_block_hash_size + BLOCK_HEAD_SIZE];
	ser_declare;
	ser_begin(buffer, 0);
	ser_int32(block->id);
	int32_t num = (int32_t) htable_size(block->LHTable);
	ser_int32(num);

	hlink* item = htable_first(block->LHTable);
	while (item) {
		ser_bytes(&item->key, sizeof(Fingerprint));
		ser_bytes(&item->value, sizeof(ContainerId));
		item = htable_next(block->LHTable);
	}

	int length = ser_length(buffer);
	if (length < (silo_block_hash_size + BLOCK_HEAD_SIZE)) {
		memset(buffer + length, 0,
				silo_block_hash_size + BLOCK_HEAD_SIZE - length);
	}

	lseek(block_vol_fd,
			sizeof(block_num)
					+ ((off_t)block_num) * (silo_block_hash_size + BLOCK_HEAD_SIZE),
			SEEK_SET);
	write(block_vol_fd, buffer, BLOCK_HEAD_SIZE + silo_block_hash_size);

	block_num++;
}

static SiloBlock* read_block_from_volume(int32_t block_id) {
	if (block_id < 0) {
		dprint("invalid id!");
		return NULL;
	}
	lseek(block_vol_fd,
			sizeof(block_num)
					+ ((off_t)block_id) * (silo_block_hash_size + BLOCK_HEAD_SIZE),
			SEEK_SET);
	char buffer[silo_block_hash_size + BLOCK_HEAD_SIZE];
	read(block_vol_fd, buffer, silo_block_hash_size + BLOCK_HEAD_SIZE);

	SiloBlock* block = silo_block_new();

	unser_declare;
	unser_begin(buffer, 0);
	unser_int32(block->id);
	if (block->id != block_id) {
		printf("%s,%d: inconsistent block id!%d != %d!\n", __FILE__, __LINE__,
				block->id, block_id);
		silo_block_free(block);
		return NULL;
	}
	int64_t num = 0;
	unser_int32(num);
	Fingerprint finger;
	ContainerId cid;
	int i;
	for (i = 0; i < num; ++i) {
		unser_bytes(&finger, sizeof(Fingerprint));
		unser_bytes(&cid, sizeof(ContainerId));
		htable_insert(block->LHTable, &finger, cid);
	}

	return block;
}

static gint block_cmp(gconstpointer a, gconstpointer b) {
	return ((SiloBlock*) a)->id - ((SiloBlock*) b)->id;
}

BOOL silo_init() {
	silo_segment_hash_size = silo_segment_size * 1024 / chunk_size
			* silo_item_size;
	silo_block_hash_size = silo_block_size * 1024 * 1024 / chunk_size
			* silo_item_size;

	char filename[256];
	/* init SHTable */
	SHTable = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, free,
			free);

	strcpy(filename, working_path);
	strcat(filename, "index/shtable");
	int fd;
	if ((fd = open(filename, O_CREAT | O_RDWR, S_IRWXU)) < 0) {
		dprint("failed to open shtable!");
		return FALSE;
	}
	int num = 0;
	if (read(fd, &num, 4) == 4) {
		/* existed, read it */
		int i;
		for (i = 0; i < num; ++i) {
			Fingerprint* representative_fingerprint = (Fingerprint*) malloc(
					sizeof(Fingerprint));
			int32_t* block_id = (int32_t*) malloc(sizeof(int32_t));
			read(fd, representative_fingerprint, sizeof(Fingerprint));
			read(fd, block_id, sizeof(int32_t));
			g_hash_table_insert(SHTable, representative_fingerprint, block_id);
		}
	}
	close(fd);

	/* init block volume */
	block_num = 0;
	strcpy(filename, working_path);
	strcat(filename, "index/block.vol");
	if ((block_vol_fd = open(filename, O_CREAT | O_RDWR, S_IRWXU)) < 0) {
		dprint("failed to open block.vol!");
		return FALSE;
	}
	if (read(block_vol_fd, &block_num, 4) != 4) {
		/* empty */
		block_num = 0;
		lseek(block_vol_fd, 0, SEEK_SET);
		write(block_vol_fd, &block_num, 4);
	}

	/*read_cache = g_sequence_new(silo_block_free);*/
	read_cache = lru_cache_new(silo_read_cache_size, block_cmp);
}

void silo_destroy() {
	if (write_buffer) {
		if (write_buffer->dirty) {
			append_block_to_volume(write_buffer);
			GHashTableIter iter;
			gpointer key, value;
			g_hash_table_iter_init(&iter, write_buffer->representative_table);
			while (g_hash_table_iter_next(&iter, &key, &value)) {
				g_hash_table_insert(SHTable, key, value);
			}
		}
		silo_block_free(write_buffer);
	}

	lru_cache_free(read_cache, silo_block_free);

	/*update block volume*/
	lseek(block_vol_fd, 0, SEEK_SET);
	write(block_vol_fd, &block_num, 4);
	close(block_vol_fd);
	block_vol_fd = -1;

	/*update shtable*/
	char filename[256];
	strcpy(filename, working_path);
	strcat(filename, "index/shtable");
	int fd;
	if ((fd = open(filename, O_CREAT | O_RDWR, S_IRWXU)) < 0) {
		dprint("failed to open shtable!");
		return;
	}
	uint32_t hash_num = g_hash_table_size(SHTable);
	write(fd, &hash_num, 4);
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, SHTable);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		write(fd, key, sizeof(Fingerprint));
		write(fd, value, sizeof(int32_t));
	}
	g_hash_table_destroy(SHTable);
	SHTable = 0;
	close(fd);
}

ContainerId silo_search(Fingerprint* fingerprint, EigenValue* eigenvalue) {
	ContainerId *cid = NULL;
	static int chunk_num = 0;

	/* Does it exist in write_buffer? */
	if (write_buffer)
		cid = htable_lookup(write_buffer->LHTable, fingerprint);

	if (eigenvalue) {
		if (chunk_num != 0)
			dprint("An error!");
		chunk_num = eigenvalue->chunk_num;
		/* Does it exist in SHTable? */
		int32_t *bid = g_hash_table_lookup(SHTable, &eigenvalue->values[0]);
		if (bid != NULL) {
			/* its block is not in read cache */
			SiloBlock sample;
			sample.id = *bid;
			SiloBlock *read_block = lru_cache_lookup(read_cache, &sample);
			if (read_block == NULL) {
				read_block = read_block_from_volume(*bid);
				SiloBlock *block = lru_cache_insert(read_cache, read_block);
				if (block)
					silo_block_free(block);
			}
		}
	}

	/* filter the fingerprint in read cache */
	SiloBlock* block = lru_cache_first(read_cache);
	while (block) {
		cid = htable_lookup(block->LHTable, fingerprint);
		if (cid)
			break;
		block = lru_cache_next(read_cache);
	}

	chunk_num--;
	if (cid && *cid < 0)
		printf("SEARCH:container id less than 0, %d\n", *cid);
	return cid == NULL ? TMP_CONTAINER_ID : *cid;
}

void silo_update(Fingerprint* fingerprint, ContainerId container_id,
		EigenValue* eigenvalue, BOOL update) {
	if (container_id < 0)
		printf("UPDATE:container id less than 0, %d\n", container_id);

	static int chunk_num = 0;

	if (eigenvalue) {
		if (chunk_num != 0)
			dprint("Error!");
		chunk_num = eigenvalue->chunk_num;
	}

	if (write_buffer == NULL)
		write_buffer = silo_block_new();

	/* Insert the representeigenvalue->chunk_num;ative fingerprint of the segment into the block */
	if (eigenvalue
			&& g_hash_table_lookup(write_buffer->representative_table,
					&eigenvalue->values[0]) == NULL) {
		/* a new segement */
		if (write_buffer->size + silo_segment_hash_size > silo_block_hash_size)
			dprint("Error! It can't happen!");
		Fingerprint *new_repfp = (Fingerprint*) malloc(sizeof(Fingerprint));
		memcpy(new_repfp, &eigenvalue->values[0], sizeof(Fingerprint));
		int32_t *new_bid = (int32_t*) malloc(sizeof(int32_t));
		*new_bid = block_num;
		g_hash_table_insert(write_buffer->representative_table, new_repfp,
				new_bid);
	}

	Fingerprint* new_finger = (Fingerprint*) malloc(sizeof(Fingerprint));
	memcpy(new_finger, fingerprint, sizeof(Fingerprint));
	ContainerId* new_cid = (ContainerId*) malloc(sizeof(ContainerId));
	memcpy(new_cid, &container_id, sizeof(ContainerId));
	if (*new_cid < 0) {
		printf("INSERT:container id less than 0, %d, %d\n", container_id,
				*new_cid);
	}
	htable_insert(write_buffer->LHTable, new_finger, *new_cid);

	write_buffer->dirty = TRUE;

	write_buffer->size += silo_item_size;
	chunk_num--;

	/* is write_buffer full? */
	if ((write_buffer->size + silo_item_size) > silo_block_hash_size) {
		if (write_buffer->size != silo_block_hash_size)
			dprint("An error!");
		if (chunk_num != 0)
			/* A block boundary must be a segment boundary */
			dprint("An error!");
		if (write_buffer->dirty) {
			/* If the block has something new. */
			append_block_to_volume(write_buffer);
			GHashTableIter iter;
			gpointer key, value;
			g_hash_table_iter_init(&iter, write_buffer->representative_table);
			while (g_hash_table_iter_next(&iter, &key, &value)) {
				g_hash_table_insert(SHTable, key, value);
			}
		}
		silo_block_free(write_buffer);
		write_buffer = NULL;
	}
}

EigenValue* extract_eigenvalue_silo(Chunk *chunk) {
	static int chunk_cnt = 0;
	static int32_t current_segment_size = 0;

	EigenValue *eigenvalue = NULL;
	if (chunk->length == FILE_END)
		return NULL;
	if (chunk->length == STREAM_END
			|| (current_segment_size + silo_item_size)
					> silo_segment_hash_size) {
		/* segment boundary is found */
		eigenvalue = (EigenValue*) malloc(
				sizeof(EigenValue) + sizeof(Fingerprint));
		/*print_finger(&representative_fingerprint);*/
		memcpy(&eigenvalue->values[0], &representative_fingerprint,
				sizeof(Fingerprint));
		eigenvalue->value_num = 1;
		memset(&representative_fingerprint, 0xff, sizeof(Fingerprint));
		current_segment_size = 0;

		eigenvalue->chunk_num = chunk_cnt;
		chunk_cnt = 0;
	}
	if (chunk->length != STREAM_END) {
		chunk_cnt++;
		if (memcmp(&chunk->hash, &representative_fingerprint,
				sizeof(Fingerprint)) < 0) {
			memcpy(&representative_fingerprint, &chunk->hash,
					sizeof(Fingerprint));
		}
		current_segment_size += silo_item_size;
	}
	return eigenvalue;
}
