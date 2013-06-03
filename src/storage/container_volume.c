/* * container_volume.c *
 *  Created on: Jun 13, 2012
 *      Author: fumin
 */
#include "../global.h"
#include "container.h"
#include "container_volume.h"
#include "../index/index.h"
#include "../jcr.h"

extern char working_path[];
/* container queue */
extern SyncQueue* container_queue;
extern int simulation_level;

ContainerVolume container_volume;

/*
 * Init and destroy functions of the globally unique container_volume.
 */
int init_container_volume() {

	strcpy(container_volume.volume_path, working_path);
	strcat(container_volume.volume_path, "container_count");

	if ((container_volume.file_descriptor = open(container_volume.volume_path,
			O_RDWR | O_CREAT, S_IRWXU)) <= 0) {
		dprint("Failed to init container manager!");
		return FAILURE;
	}

	int32_t count = 0;
	if (read(container_volume.file_descriptor, &count, 4) == 4) {
		container_volume.container_num = count;
	} else {
		container_volume.container_num = 0;
	}

	close(container_volume.file_descriptor);

	strcpy(container_volume.volume_path, working_path);
	strcat(container_volume.volume_path, "container_volume");

	if ((container_volume.file_descriptor = open(container_volume.volume_path,
			O_RDWR | O_CREAT /*| O_DIRECT*/, S_IRWXU)) <= 0) {
		dprint("Failed to init container manager!");
		return FAILURE;
	}

	pthread_mutex_init(&container_volume.mutex, 0);
	return SUCCESS;
}

int update_container_volume() {
	if (pthread_mutex_lock(&container_volume.mutex)) {
		printf("%s, %d: failed to lock!\n", __FILE__, __LINE__);
	}
	strcpy(container_volume.volume_path, working_path);
	strcat(container_volume.volume_path, "container_count");

	int fd;
	if ((fd = open(container_volume.volume_path, O_RDWR | O_CREAT, S_IRWXU))
			<= 0) {
		dprint("Failed to init container manager!");
		return FAILURE;
	}
	int len = write(fd, &container_volume.container_num, 4);
	close(fd);

	pthread_mutex_unlock(&container_volume.mutex);
	if (len != 4)
		puts("Failed to write container volume descriptor!");
	return SUCCESS;
}

int destroy_container_volume() {
	update_container_volume();
	pthread_mutex_destroy(&container_volume.mutex);
	close(container_volume.file_descriptor);
	return SUCCESS;
}

static void set_container_id(Container *container) {
	container->id = container_volume.container_num;
}

Container *create_container() {
	Container *new_one = container_new_full();
	set_container_id(new_one);
	return new_one;
}

ContainerId seal_container(Container* container) {
	int32_t chunknum = container_get_chunk_num(container);
	if (chunknum == 0) {
		container_free_full(container);
		return TMP_CONTAINER_ID;
	}
	container_volume.container_num++;
	return container->id;
}

static char* container_ser_meta(Container* container) {
	if ((g_hash_table_size(container->meta) * CONTAINER_META_ENTRY_SIZE
			+ CONTAINER_DES_SIZE) > CONTAINER_MAX_META_SIZE) {
		dprint("Meta Overflow!");
		return 0;
	}

	char *buff;
	int ret = posix_memalign(&buff, getpagesize(), CONTAINER_SIZE);

	ser_declare;
	ser_begin(buff, 0);
	ser_int32(container->id);
	ser_int32(container->chunk_num);
	ser_int32(container->data_size);

	GHashTableIter iter;
	g_hash_table_iter_init(&iter, container->meta);
	gpointer key, value;
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		ContainerMetaEntry *cm = (ContainerMetaEntry*) value;
		ser_bytes(&cm->hash, sizeof(Fingerprint));
		ser_int32(cm->offset);
		ser_int32(cm->length);
	}

	if (ser_length(buff) < CONTAINER_MAX_META_SIZE) {
		memset(buff + ser_length(buff), 0,
				CONTAINER_MAX_META_SIZE - ser_length(buff));
	}
	return buff;
}
/*
 * serialize container into bit string.
 * unserialize bit string into hash table.
 */
static char* container_ser(Container* container) {
	if ((g_hash_table_size(container->meta) * CONTAINER_META_ENTRY_SIZE
			+ CONTAINER_DES_SIZE) > CONTAINER_MAX_META_SIZE) {
		dprint("Meta Overflow!");
		return 0;
	}
	/*char *buff = malloc(CONTAINER_SIZE);*/
	char *buff;
	int ret = posix_memalign(&buff, getpagesize(), CONTAINER_SIZE);

	ser_declare;
	ser_begin(buff, 0);
	ser_int32(container->id);
	ser_int32(container->chunk_num);
	ser_int32(container->data_size);

	GHashTableIter iter;
	g_hash_table_iter_init(&iter, container->meta);
	gpointer key, value;
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		ContainerMetaEntry *cm = (ContainerMetaEntry*) value;
		ser_bytes(&cm->hash, sizeof(Fingerprint));
		ser_int32(cm->offset);
		ser_int32(cm->length);
	}
	ser_bytes(container->data, container->data_size);
	if (ser_length(buff) > CONTAINER_SIZE) {
		printf("%s, %d: container %d is overflow.\n", __FILE__, __LINE__,
				container->id);
	}
	if (ser_length(buff) < CONTAINER_SIZE) {
		memset(buff + ser_length(buff), 0, CONTAINER_SIZE - ser_length(buff));
	}
	return buff;
}

static Container* container_unser_meta(char* buff, int32_t size) {
	Container* container = container_new_meta_only();
	unser_declare;
	unser_begin(buff, 0);
	unser_int32(container->id);
	unser_int32(container->chunk_num);
	unser_int32(container->data_size);
	if (container->chunk_num < 0) {
		dprint("This container is broken!");
		container_free_full(container);
		return 0;
	}
	if ((container->chunk_num * CONTAINER_META_ENTRY_SIZE + CONTAINER_DES_SIZE)
			> size) {
		printf("id=%d, num: %d, size=%d\n", container->id, container->chunk_num,
				size);
	}
	int i = 0;
	for (; i < container->chunk_num; ++i) {
		ContainerMetaEntry *cm = (ContainerMetaEntry*) malloc(
				sizeof(ContainerMetaEntry));
		unser_bytes(&cm->hash, sizeof(Fingerprint));
		unser_int32(cm->offset);
		unser_int32(cm->length);
		g_hash_table_replace(container->meta, cm->hash, cm);
	}
	return container;
}

static Container* container_unser_full(char* buff, int32_t size) {
	Container* container = container_new_full();
	unser_declare;
	unser_begin(buff, 0);
	unser_int32(container->id);
	unser_int32(container->chunk_num);
	unser_int32(container->data_size);
	int i = 0;
	for (; i < container->chunk_num; ++i) {
		ContainerMetaEntry *cm = (ContainerMetaEntry*) malloc(
				sizeof(ContainerMetaEntry));
		unser_bytes(&cm->hash, sizeof(Fingerprint));
		unser_int32(cm->offset);
		unser_int32(cm->length);
		g_hash_table_replace(container->meta, cm->hash, cm);
	}

	unser_bytes(container->data, container->data_size);
	return container;
}

/*
 * Functions provided for Reading and Writing container volume.
 */
BOOL append_container(Container* container) {
	if (container->chunk_num == 0) {
		puts("append a empty container!");
		return FAILURE;
	}

	char *buffer = 0;
	if (simulation_level < SIMULATION_APPEND) {
		/* The simulation level is below APPEND */
		check_container(container);
		buffer = container_ser(container);
	} else {
		/* simulate appending */
		buffer = container_ser_meta(container);
	}
	int len = 0;
	while (TRUE) {
		if (pthread_mutex_lock(&container_volume.mutex)) {
			dprint("failed to lock!\n");
		}

		if (simulation_level < SIMULATION_APPEND) {
			if (lseek(container_volume.file_descriptor,
					((off_t) container->id) * CONTAINER_SIZE, SEEK_SET) == -1) {
				dprint("seal container lseek error!\n");
				free(buffer);
				pthread_mutex_unlock(&container_volume.mutex);
				return FALSE;
			}
			len = write(container_volume.file_descriptor, buffer,
					CONTAINER_SIZE);
			if (len != CONTAINER_SIZE) {
				puts("Failed to seal container! Retry!");
				pthread_mutex_unlock(&container_volume.mutex);
				continue;
			}
		} else {
			if (lseek(container_volume.file_descriptor,
					((off_t) container->id) * CONTAINER_MAX_META_SIZE, SEEK_SET)
					== -1) {
				dprint("seal container lseek error!\n");
				free(buffer);
				pthread_mutex_unlock(&container_volume.mutex);
				return FALSE;
			}
			len = write(container_volume.file_descriptor, buffer,
					CONTAINER_MAX_META_SIZE);
			if (len != CONTAINER_MAX_META_SIZE) {
				dprint("Failed to seal container! Retry!");
				pthread_mutex_unlock(&container_volume.mutex);
				continue;
			}
		}

		pthread_mutex_unlock(&container_volume.mutex);
		break;
	}

	free(buffer);
	return TRUE;
}

/*
 * open series functions are used to read a container from volume fully or partially.
 */
Container *read_container_meta_only(ContainerId id) {
	if (id < 0) {
		printf("%s, %d: container id < 0.\n", __FILE__, __LINE__);
		return NULL ;
	}
	/*char buff[CONTAINER_MAX_META_SIZE];*/
	char *buff;
	int ret = posix_memalign(&buff, getpagesize(), CONTAINER_MAX_META_SIZE);

	if (pthread_mutex_lock(&container_volume.mutex)) {
		printf("%s, %d: failed to lock!\n", __FILE__, __LINE__);
	}

	if (simulation_level < SIMULATION_APPEND)
		lseek(container_volume.file_descriptor, (off_t) id * CONTAINER_SIZE,
				SEEK_SET);
	else
		lseek(container_volume.file_descriptor,
				(off_t) id * CONTAINER_MAX_META_SIZE, SEEK_SET);

	int len = read(container_volume.file_descriptor, buff,
			CONTAINER_MAX_META_SIZE);
	if (len != (CONTAINER_MAX_META_SIZE)) {
		pthread_mutex_unlock(&container_volume.mutex);
		dprint("The read container for DDFS cache has not been written yet!");
		return 0;
	}

	pthread_mutex_unlock(&container_volume.mutex);
	Container *container = container_unser_meta(buff, len);
	free(buff);
	return container;
}

Container* read_container(ContainerId id) {
	if (id < 0) {
		printf("%s, %d: container id < 0.\n", __FILE__, __LINE__);
		return NULL ;
	}
	if (pthread_mutex_lock(&container_volume.mutex)) {
		printf("%s, %d: failed to lock!\n", __FILE__, __LINE__);
	}
	lseek(container_volume.file_descriptor, (off_t) id * CONTAINER_SIZE,
			SEEK_SET);
	char *buff;
	int ret = posix_memalign(&buff, getpagesize(), CONTAINER_SIZE);

	int len = read(container_volume.file_descriptor, buff, CONTAINER_SIZE);
	pthread_mutex_unlock(&container_volume.mutex);
	if (len != CONTAINER_SIZE) {
		free(buff);
		return 0;
	}

	Container* container = container_unser_full(buff, len);
	free(buff);
	if (simulation_level == SIMULATION_NO)
		check_container(container);
	return container;
}

