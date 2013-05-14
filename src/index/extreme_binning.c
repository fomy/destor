/**
 * @file extreme_binning.c
 * @Synopsis MASCOTS'09, EXtreme Binning without file-level dedup.
 *
 * @author fumin, fumin@hust.edu.cn
 * @version 1
 * @date 2013-01-09
 */
#include "../dedup.h"
#include "extreme_binning.h" 
#include "../jcr.h"

#define BIN_LEVEL_COUNT 20
const static int64_t bvolume_head_size = 20;
const static int64_t level_factor = 512;

/* representative fingerprint and size */
const static int32_t bin_head_size = 24;

/* representative fingerprint : bin_address pairs */
static GHashTable *primary_index;
static BinVolume *bin_volume_array[BIN_LEVEL_COUNT];

static Fingerprint representative_fingerprint;

/* searchable bin queue */
static Queue *bin_queue;
static GHashTable* bin_table;

extern char working_path[];

static BinVolume* bin_volume_init(int64_t level) {
	BinVolume* bvol = (BinVolume*) malloc(sizeof(BinVolume));
	bvol->level = level;
	bvol->current_bin_num = 0;
	bvol->current_volume_length = bvolume_head_size;

	char volname[20];
	sprintf(volname, "bvol%ld", level);
	strcpy(bvol->filename, working_path);
	strcat(bvol->filename, "index/");
	strcat(bvol->filename, volname);

	int fd;
	if ((fd = open(bvol->filename, O_CREAT | O_RDWR, S_IRWXU)) < 0) {
		printf("%s, %d:failed to open bin volume %ld\n", __FILE__, __LINE__,
				level);
		return bvol;
	}

	int64_t tmp = -1;
	if (read(fd, &tmp, 8) != 8 || tmp != level) {
		printf("%s, %d: read an empty bin volume.\n", __FILE__, __LINE__);
		lseek(fd, 0, SEEK_SET);
		write(fd, &level, 8);
		write(fd, &bvol->current_bin_num, 4);
		write(fd, &bvol->current_volume_length, 8);
		close(fd);
		return bvol;
	}
	read(fd, &bvol->current_bin_num, 4);
	read(fd, &bvol->current_volume_length, 8);
	close(fd);
	return bvol;
}

static BOOL bin_volume_destroy(BinVolume *bvol) {
	if (!bvol)
		return FALSE;
	int fd;
	if ((fd = open(bvol->filename, O_CREAT | O_RDWR, S_IRWXU)) < 0) {
		printf("%s, %d: failed to open bin volume %ld\n", __FILE__, __LINE__,
				bvol->level);
		return FALSE;
	}
	lseek(fd, 0, SEEK_SET);
	write(fd, &bvol->level, 8);
	write(fd, &bvol->current_bin_num, 4);
	write(fd, &bvol->current_volume_length, 8);

	free(bvol);
	return TRUE;
}

#define BIN_ADDR_MASK (0xffffffffffffff)

/*
 * addr == 0 means this is really a new bin.
 */
static Bin *bin_new(int64_t addr, Fingerprint *representative_fingerprint) {
	Bin *nbin = (Bin*) malloc(sizeof(Bin));

	nbin->address = addr;
	nbin->dirty = FALSE;
	nbin->reference = 0;
	memcpy(&nbin->representative_fingerprint, representative_fingerprint,
			sizeof(Fingerprint));

	nbin->fingers = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
			free, free);
	return nbin;
}

static void bin_free(Bin *bin) {
	g_hash_table_destroy(bin->fingers);
	bin->fingers = 0;
	free(bin);
}

static int64_t no_to_level(int64_t chunk_num) {
	/* 24 + count*(20+4 +1) */
	int64_t bin_size = bin_head_size + chunk_num * 25;
	bin_size /= level_factor;
	int level = 0;
	while (bin_size) {
		++level;
		bin_size >>= 1;
	}
	return level;
}

static int64_t level_to_size(int64_t level) {
	if (level < 0 || level >= BIN_LEVEL_COUNT) {
		printf("%s, %d: invalid level %ld\n", __FILE__, __LINE__, level);
		return -1;
	}
	int64_t size = (1 << level) * level_factor;
	return size;
}

static int64_t write_bin_to_volume(Bin *bin) {
	int64_t level = bin->address >> 56;
	int64_t offset = bin->address & BIN_ADDR_MASK;

	int64_t new_level = no_to_level(g_hash_table_size(bin->fingers));
	if (new_level != level) {
		printf("%s, %d: level up %ld -> %ld\n", __FILE__, __LINE__, level,
				new_level);
		offset = 0;
	}

	char *buffer = malloc(level_to_size(new_level));
	ser_declare;
	ser_begin(buffer, 0);
	ser_bytes(&bin->representative_fingerprint, sizeof(Fingerprint));
	ser_int32(g_hash_table_size(bin->fingers));

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, bin->fingers);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		ser_bytes(key, sizeof(Fingerprint));
		ser_bytes(value, sizeof(ContainerId));
		ser_int8('\t');
	}

	if (ser_length(buffer) < level_to_size(new_level)) {
		memset(buffer + ser_length(buffer), 0xcf,
				level_to_size(new_level) - ser_length(buffer));
	}

	BinVolume *bvol = bin_volume_array[new_level];
	if (offset == 0) {
		offset = bvol->current_volume_length;
	}
	int fd = open(bvol->filename, O_RDWR);
	lseek(fd, offset, SEEK_SET);
	if (level_to_size(new_level)
			!= write(fd, buffer, level_to_size(new_level))) {
		dprint("failed to write bin!");
		close(fd);
		free(buffer);
		return 0;
	}
	close(fd);
	bvol->current_bin_num++;
	bvol->current_volume_length += level_to_size(new_level);
	free(buffer);
	bin->dirty = FALSE;
	return (new_level << 56) + offset;
}

static Bin* read_bin_from_volume(int64_t address) {
	if (address == 0) {
		return NULL ;
	}
	int64_t level = address >> 56;
	int64_t offset = address & BIN_ADDR_MASK;

	BinVolume *bvol = bin_volume_array[level];
	char *buffer = malloc(level_to_size(level));

	int fd = open(bvol->filename, O_RDWR);
	lseek(fd, offset, SEEK_SET);
	read(fd, buffer, level_to_size(level));
	close(fd);

	unser_declare;
	unser_begin(buffer, 0);
	Fingerprint representative_fingerprint;
	int32_t chunk_num;
	unser_bytes(&representative_fingerprint, sizeof(Fingerprint));
	unser_int32(chunk_num);

	Bin *bin = bin_new(address, &representative_fingerprint);
	int i;
	for (i = 0; i < chunk_num; ++i) {
		Fingerprint *finger = (Fingerprint*) malloc(sizeof(Fingerprint));
		ContainerId *cid = (ContainerId*) malloc(sizeof(ContainerId));
		unser_bytes(finger, sizeof(Fingerprint));
		unser_bytes(cid, sizeof(ContainerId));
		char tmp;
		unser_int8(tmp);
		if (tmp != '\t')
			dprint("corrupted bin!");
		g_hash_table_insert(bin->fingers, finger, cid);
	}
	free(buffer);

	bin->dirty = FALSE;
	return bin;
}

BOOL extreme_binning_init() {
	primary_index = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
			NULL, free);

	char filename[256];
	strcpy(filename, working_path);
	strcat(filename, "index/primary_index.map");

	int fd;
	if ((fd = open(filename, O_CREAT | O_RDWR, S_IRWXU)) < 0) {
		dprint("failed to open primary_index.map");
		return FALSE;
	}

	int item_num = 0;
	read(fd, &item_num, 4);
	int i = 0;
	for (; i < item_num; ++i) {
		PrimaryItem *item = (PrimaryItem*) malloc(sizeof(PrimaryItem));
		read(fd, &item->representative_fingerprint, sizeof(Fingerprint));
		read(fd, &item->bin_addr, sizeof(item->bin_addr));
		g_hash_table_insert(primary_index, &item->representative_fingerprint,
				item);
	}
	close(fd);

	i = 0;
	for (; i < BIN_LEVEL_COUNT; ++i) {
		bin_volume_array[i] = bin_volume_init((int64_t) i);
	}

	memset(&representative_fingerprint, 0xff, sizeof(Fingerprint));

	bin_queue = queue_new();
	bin_table = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL,
			bin_free);
	return TRUE;
}

void extreme_binning_destroy() {
	if (queue_size(bin_queue) || g_hash_table_size(bin_table))
		dprint("Error! bin queue/table is not empty!");
	queue_free(bin_queue, bin_free);
	g_hash_table_destroy(bin_table);

	char filename[256];
	strcpy(filename, working_path);
	strcat(filename, "index/primary_index.map");

	int fd;
	if ((fd = open(filename, O_CREAT | O_RDWR, S_IRWXU)) < 0) {
		dprint("failed to open primary_index.map");
		return;
	}

	int item_num = g_hash_table_size(primary_index);
	write(fd, &item_num, 4);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, primary_index);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		PrimaryItem* item = (PrimaryItem*) value;
		write(fd, &item->representative_fingerprint, sizeof(Fingerprint));
		write(fd, &item->bin_addr, sizeof(item->bin_addr));
	}
	close(fd);
	g_hash_table_destroy(primary_index);

	int i = 0;
	for (; i < BIN_LEVEL_COUNT; ++i) {
		bin_volume_destroy(bin_volume_array[i]);
		bin_volume_array[i] = 0;
	}
}

ContainerId extreme_binning_search(Fingerprint *fingerprint,
		EigenValue* eigenvalue) {
	/* current active bin
	 * the tail of bin queue */
	static Bin *current_bin = NULL;

	if ((current_bin == 0) || eigenvalue) {
		current_bin = g_hash_table_lookup(bin_table, &eigenvalue->values[0]);
		if (current_bin)
			current_bin->reference++;
		else {
			/* read bin according to representative fingerprint */
			PrimaryItem* item = g_hash_table_lookup(primary_index,
					&eigenvalue->values[0]);
			if (item) {
				current_bin = read_bin_from_volume(item->bin_addr);
				if (memcmp(current_bin->representative_fingerprint,
						&eigenvalue->values[0], sizeof(Fingerprint)) != 0) {
					puts("error");
				}
			} else {
				current_bin = bin_new(0, &eigenvalue->values[0]);
			}
			g_hash_table_insert(bin_table,
					&current_bin->representative_fingerprint, current_bin);
		}

		queue_push(bin_queue, current_bin);
	}

	ContainerId *cid = g_hash_table_lookup(current_bin->fingers, fingerprint);

	if (cid) {
		return *cid;
	} else {
		return TMP_CONTAINER_ID;
	}
}

void extreme_binning_update(Fingerprint *finger, ContainerId container_id,
		EigenValue* eigenvalue, BOOL update) {
	/* The head of bin queue */
	static Bin *current_bin = NULL;
	static int chunk_num = 0;

	if (eigenvalue) {
		/* A new file */
		if (current_bin)
			dprint("An error! current bin is not NULL!");
		current_bin = queue_top(bin_queue);
		chunk_num = eigenvalue->chunk_num;
	}

	if (update) {
		Fingerprint *new_finger = (Fingerprint*) malloc(sizeof(Fingerprint));
		memcpy(new_finger, finger, sizeof(Fingerprint));
		ContainerId* new_id = (ContainerId*) malloc(sizeof(ContainerId));
		*new_id = container_id;
		g_hash_table_insert(current_bin->fingers, new_finger, new_id);
		current_bin->dirty = TRUE;
	}

	chunk_num--;
	if (chunk_num == 0) {
		/* The file is finished */
		current_bin = queue_pop(bin_queue);
		if (current_bin->reference > 0) {
			current_bin->reference--;
		} else {
			if (current_bin->dirty == TRUE) {
				int64_t new_addr = write_bin_to_volume(current_bin);
				if (new_addr != current_bin->address) {
					PrimaryItem* item = g_hash_table_lookup(primary_index,
							&current_bin->representative_fingerprint);
					if (item == NULL ) {
						item = (PrimaryItem*) malloc(sizeof(PrimaryItem));
						memcpy(&item->representative_fingerprint,
								&current_bin->representative_fingerprint,
								sizeof(Fingerprint));
						g_hash_table_insert(primary_index,
								&item->representative_fingerprint, item);
					}
					item->bin_addr = new_addr;
				}
			}
			g_hash_table_remove(bin_table,
					&current_bin->representative_fingerprint);
		}
		current_bin = NULL;
	}
}

EigenValue* extract_eigenvalue_exbin(Chunk *chunk) {
	static int cnt = 0;
	if (chunk->length == FILE_END) {
		EigenValue *eigenvalue = (EigenValue*) malloc(
				sizeof(EigenValue) + sizeof(Fingerprint));
		memcpy(&eigenvalue->values[0], &representative_fingerprint,
				sizeof(Fingerprint));
		eigenvalue->chunk_num = cnt;
		eigenvalue->value_num = 1;
		memset(&representative_fingerprint, 0xff, sizeof(Fingerprint));
		cnt = 0;
		return eigenvalue;
	}

	/* normal chunk.
	 * STREAM_END is meaningless to extreme binning. */
	if (chunk->length != STREAM_END
			&& memcmp(&chunk->hash, &representative_fingerprint,
					sizeof(Fingerprint)) < 0) {
		memcpy(&representative_fingerprint, &chunk->hash, sizeof(Fingerprint));
	}
	cnt++;
	if (chunk->data) {
		free(chunk->data);
		chunk->data = NULL;
	}
	return NULL ;
}
