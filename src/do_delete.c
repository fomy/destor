/*
 * delete_server.c
 *
 *  Created on: Jun 21, 2012
 *      Author: fumin
 */
#include "destor.h"
#include "storage/containerstore.h"
#include "index/index.h"

/*
 * delete all jobs before jobid, including itself.
 * Find all containers in manifest whose time is earlier than jobid.
 * These containers can be deleted.
 * Read the metadata part of these containers,
 * and delete entries in fingerprint index.
 */
void do_delete(int jobid) {

	if (destor.index_specific != INDEX_SPECIFIC_RAM) {
		destor_log(DESTOR_WARNING, "Deletion does not support the index.");
		exit(1);
	}

	init_index();

	GHashTable *delete_containers = g_hash_table_new_full(g_int_hash,
			g_int_equal, NULL, free);
	int manifest_size = 0;
	if (rewriting_algorithm != CUMULUS) {
		GHashTable *container_manifest = read_container_manifest();
		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, container_manifest);
		while (g_hash_table_iter_next(&iter, &key, &value)) {
			ContainerUsage* elem = (ContainerUsage*) value;
			if (elem->read_size <= jobid) {
				/*delete it*/
				ContainerId *cid = (ContainerId*) malloc(sizeof(ContainerId));
				*cid = elem->cntnr_id;
				g_hash_table_insert(delete_containers, cid, cid);
			}
		}
		manifest_size = g_hash_table_size(container_manifest);
		update_container_manifest(container_manifest, jobid);
		g_hash_table_destroy(container_manifest);
	} else {
		delete_containers = load_deleted_containers(jobid, kept_versions);
	}

	int32_t delete_container_num = 0;
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, delete_containers);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		Container* delete_container = read_container_meta_only(
				*(ContainerId*) key);
		int32_t chunk_num = container_get_chunk_num(delete_container);
		Fingerprint *fps = container_get_all_fingers(delete_container);
		int i = 0;
		for (; i < chunk_num; ++i) {
			ContainerId id = index_search(&fps[i], NULL);
			if (id == delete_container->id)
				index_delete(&fps[i]);
		}

		free(fps);
		container_free_full(delete_container);
		delete_container_num++;
	}

	destor_stat->deleted_container_num += delete_container_num;
	g_hash_table_destroy(delete_containers);

	index_destroy();

	if (rewriting_algorithm != CUMULUS) {
		char logfile[] = "delete.log";
		int fd = open(logfile, O_WRONLY | O_CREAT, S_IRWXU);
		lseek(fd, 0, SEEK_END);
		char buf[100];

		/*
		 * number of containers deleted
		 * number of live containers
		 */
		sprintf(buf, "%d %d\n", delete_container_num,
				manifest_size - delete_container_num);
		write(fd, buf, strlen(buf));
		close(fd);
	}
	return SUCCESS;
}
