/*
 * container.c
 *
 *  Created on: Sep 20, 2012
 *      Author: fumin
 */

/*#include "container_storage.h"*/
#include "container.h"
extern int simulation_level;

inline static int32_t container_size(Container* container) {
	int32_t size = CONTAINER_DES_SIZE;
	size += (container->chunk_num * CONTAINER_META_ENTRY_SIZE);
	size += container->data_size;
	return size;
}

/*
 * Some functions provided for creating, sealing and reading container.
 */
Container* container_new_meta_only() {
	Container* new_one = (Container*) malloc(sizeof(Container));
	new_one->id = TMP_CONTAINER_ID;
	new_one->data_size = 0;
	new_one->chunk_num = 0;
	new_one->used_size = CONTAINER_DES_SIZE;

	new_one->meta = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
			NULL, free);
	new_one->data = 0;

	return new_one;
}

Container* container_new_full() {
	Container* new_one = container_new_meta_only();
	new_one->data = (char*) malloc(CONTAINER_SIZE);
	return new_one;
}

void container_free_full(Container* container) {
	if (container->data)
		free(container->data);
	g_hash_table_destroy(container->meta);
	free(container);
}

/*
 * If the container will over-flow after inserting.
 */
static BOOL container_overflow_predict(Container* container, int32_t length) {
	if (((container->chunk_num + 1) * CONTAINER_META_ENTRY_SIZE
			+ CONTAINER_DES_SIZE) > CONTAINER_MAX_META_SIZE) {
		dprint(
				"The metadata buffer of container_buffer is now facing overflow!");
		return TRUE;
	}
	if ((CONTAINER_META_ENTRY_SIZE + length + container_size(container))
			> CONTAINER_SIZE)
		return TRUE;
	return FALSE;
}

/*
 * Get the fingerprint array from container.
 */
Fingerprint *container_get_all_fingers(Container* container) {
	Fingerprint* fingers = (Fingerprint*) malloc(
			sizeof(Fingerprint) * container->chunk_num);
	int32_t num = 0;
	GHashTableIter iter;
	g_hash_table_iter_init(&iter, container->meta);
	gpointer key, value;
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		memcpy(&fingers[num], key, sizeof(Fingerprint));
		num++;
	}
	if (num != container->chunk_num) {
		printf("%s, %d: corrupted container\n", __FILE__, __LINE__);
	}
	return fingers;
}

/*
 * Get the usage of the container.
 */
double container_get_usage(Container* container) {
	double usage = ((double) container_size(container)) / CONTAINER_SIZE;
	return usage;
}

int32_t container_add_chunk(Container* container, Chunk* chunk) {
	if (simulation_level < SIMULATION_APPEND) {
		if (check_chunk(chunk) == FALSE) {
			dprint("invalid chunk!");
		}
	}
	if (container_contains(container, &chunk->hash)) {
		/*printf("%s, %d: Already exists!\n",__FILE__,__LINE__);*/
		return SUCCESS;
	}

	if (container_overflow_predict(container, chunk->length) == TRUE) {
		/*printf("%s, %d: container is full.\n",__FILE__,__LINE__);*/
		return CONTAINER_FULL;
	}

	ContainerMetaEntry *cm = (ContainerMetaEntry*) malloc(
			sizeof(ContainerMetaEntry));
	memcpy(&cm->hash, &chunk->hash, sizeof(Fingerprint));
	cm->offset = container->data_size;
	cm->length = chunk->length;

	g_hash_table_insert(container->meta, &cm->hash, cm);

	memcpy(container->data + container->data_size, chunk->data, chunk->length);
	container->data_size += chunk->length;
	++container->chunk_num;

	return SUCCESS;
}

BOOL check_chunk(Chunk *chunk) {
	SHA_CTX ctx;
	SHA_Init(&ctx);
	SHA_Update(&ctx, chunk->data, chunk->length);
	Fingerprint fp;
	SHA_Final(fp, &ctx);
	if (memcmp(&fp, &chunk->hash, sizeof(Fingerprint)) != 0) {
		printf("%s, %d: Chunk has been corrupted!\n", __FILE__, __LINE__);
		return FALSE;
	}
	return TRUE;
}

BOOL check_container(Container *container) {
	int chunknum = container_get_chunk_num(container);
	Fingerprint *fingers = container_get_all_fingers(container);
	int i = 0;
	for (; i < chunknum; ++i) {
		Chunk* chunk = container_get_chunk(container, &fingers[i]);
		if (check_chunk(chunk) == FALSE) {
			printf("%s, %d: container %d has been corrupted!\n", __FILE__,
					__LINE__, container->id);
			return FALSE;
		}
		free_chunk(chunk);
	}
	free(fingers);
	return TRUE;
}

ContainerMetaEntry* container_lookup(Container *container, Fingerprint* finger) {
	return g_hash_table_lookup(container->meta, finger);
}

Chunk* container_get_chunk(Container* container, Fingerprint *hash) {
	if (simulation_level == SIMULATION_NO) {
		if (!container->data) {
			dprint("Failed to get a chunk!");
			return NULL;
		}
	}
	ContainerMetaEntry *cm = container_lookup(container, hash);
	if (!cm) {
		/*dprint("Failed to get a chunk!");*/
		return NULL;
	}
	Chunk *chunk = allocate_chunk();

	chunk->data = malloc(cm->length);
	if (simulation_level >= SIMULATION_RECOVERY) {
		memset(chunk->data, 0, cm->length);
	} else {
		memcpy(chunk->data, (char*) container->data + cm->offset, cm->length);
	}
	chunk->length = cm->length;
	memcpy(&chunk->hash, &cm->hash, sizeof(Fingerprint));

	return chunk;
}

BOOL container_contains(Container* container, Fingerprint* finger) {
	return g_hash_table_lookup(container->meta, finger) == NULL ? FALSE : TRUE;
}

/* ascending order */
gint container_cmp_asc(gconstpointer a, gconstpointer b) {
	return ((Container*) a)->id - ((Container*) b)->id;
}

/* descending order */
gint container_cmp_des(gconstpointer a, gconstpointer b, gpointer user_data) {
	return ((Container*) b)->id - ((Container*) a)->id;
}

int32_t container_get_chunk_num(Container* container) {
	return container->chunk_num;
}
