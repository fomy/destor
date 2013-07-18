/**
 * @file container_usage_monitor.c
 * @Synopsis  
 * @author fumin, fumin@hust.edu.cn
 * @version 1
 * @date 2012-12-12
 */
#include "container_usage_monitor.h"

extern char working_path[];

extern double hbr_usage_threshold;
extern int rewriting_algorithm;

static ContainerUsage* container_usage_new(ContainerId cntnr_id) {
	ContainerUsage* cntnr_usg = (ContainerUsage*) malloc(
			sizeof(ContainerUsage));
	cntnr_usg->cntnr_id = cntnr_id;
	cntnr_usg->read_size = CONTAINER_DES_SIZE;
	return cntnr_usg;
}

static void container_usage_free(ContainerUsage* cntnr_usg) {
	free(cntnr_usg);
}

static double container_usage_calc(ContainerUsage* cntnr_usg) {
	return cntnr_usg->read_size / (double) CONTAINER_SIZE;
}

static double container_usage_add(ContainerUsage* cntnr_usg,
		Fingerprint* fngrprnt, int32_t chnk_lngth) {
	cntnr_usg->read_size += chnk_lngth + CONTAINER_META_ENTRY_SIZE;
	return container_usage_calc(cntnr_usg);
}

/*
 * monitor interfaces:
 * new, free, update
 */
ContainerUsageMonitor* container_usage_monitor_new() {
	ContainerUsageMonitor* cntnr_usg_mntr = (ContainerUsageMonitor*) malloc(
			sizeof(ContainerUsageMonitor));

	cntnr_usg_mntr->dense_map = g_hash_table_new_full(g_int_hash, g_int_equal,
			NULL, container_usage_free);
	cntnr_usg_mntr->sparse_map = g_hash_table_new_full(g_int_hash, g_int_equal,
			NULL, container_usage_free);

	return cntnr_usg_mntr;
}

void container_usage_monitor_free(ContainerUsageMonitor* cntnr_usg_mntr) {
	g_hash_table_destroy(cntnr_usg_mntr->dense_map);
	g_hash_table_destroy(cntnr_usg_mntr->sparse_map);

	free(cntnr_usg_mntr);
}

/* --------------------------------------------------------------------------*/
/**
 * @Synopsis 
 *      Used to monitor the external fragmentation level of the current job.
 *
 *      Once destor_filter() get the id of the container containing the chunk, 
 *      we call this function to update container usage. 
 *      If this container is in dense_map, its usage is updated.
 *      Else if the usage of the container is incresed to a level larger than threshold,
 *      it will be removed from sparse_map and be inserted into dense_map.
 *
 * @Param cntnr_usg_mntr, pointer to monitor
 * @Param cntnr_id, container id
 * @Param fngr, fingerprint of the chunk
 * @Param len, length of the chunk
 */
/* ----------------------------------------------------------------------------*/
void container_usage_monitor_update(ContainerUsageMonitor* cntnr_usg_mntr,
		ContainerId cntnr_id, Fingerprint *fngr, int32_t len) {
	ContainerUsage* cntnr_usg = g_hash_table_lookup(cntnr_usg_mntr->dense_map,
			&cntnr_id);
	if (cntnr_usg) {
		container_usage_add(cntnr_usg, NULL, len);
	} else {
		ContainerUsage *cntnr_usg = g_hash_table_lookup(
				cntnr_usg_mntr->sparse_map, &cntnr_id);
		if (!cntnr_usg) {
			cntnr_usg = container_usage_new(cntnr_id);
			g_hash_table_insert(cntnr_usg_mntr->sparse_map,
					&cntnr_usg->cntnr_id, cntnr_usg);
		}
		double usage = container_usage_add(cntnr_usg, fngr, len);
		if (usage > hbr_usage_threshold) {
			g_hash_table_steal(cntnr_usg_mntr->sparse_map, &cntnr_id);
			g_hash_table_insert(cntnr_usg_mntr->dense_map, &cntnr_usg->cntnr_id,
					cntnr_usg);
		}
	}
}

GHashTable* read_container_manifest() {
	GHashTable *container_manifest = g_hash_table_new_full(g_int_hash,
			g_int_equal, NULL, container_usage_free);

	char path[500];
	strcpy(path, working_path);
	strcat(path, "jobs/container_manifest");

	FILE *manifest_file = fopen(path, "r+");

	/* read the old file */
	if (manifest_file) {
		ContainerId id;
		int32_t time;
		while (fscanf(manifest_file, "%d:%d", &id, &time) != EOF) {
			ContainerUsage* usage = (ContainerUsage*) malloc(
					sizeof(ContainerUsage));
			usage->cntnr_id = id;
			/* We consider the read_size as time for convenience */
			usage->read_size = time;
			g_hash_table_insert(container_manifest, &usage->cntnr_id, usage);
		}
		fclose(manifest_file);
	}

	return container_manifest;
}

/* only update container with a time later than jobid.*/
void update_container_manifest(GHashTable* container_manifest, int32_t jobid) {
	char path[500];
	strcpy(path, working_path);
	strcat(path, "jobs/container_manifest");

	FILE *manifest_file = fopen(path, "w+");

	if (manifest_file == NULL) {
		dprint("can not open manifest_file");
		return;
	}

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, container_manifest);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		ContainerUsage* cntnr_usg = (ContainerUsage*) value;
		if (cntnr_usg->read_size > jobid)
			fprintf(manifest_file, "%d:%d\n", cntnr_usg->cntnr_id,
					cntnr_usg->read_size);
	}
}

/* generate new manifest of used containers after backup */
int container_usage_monitor_update_manifest(
		ContainerUsageMonitor *cntnr_usg_mntr, int32_t job_id) {
	GHashTable *container_manifest = read_container_manifest();

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, cntnr_usg_mntr->sparse_map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		ContainerUsage *old_time = g_hash_table_lookup(container_manifest, key);
		if (old_time == NULL) {
			old_time = container_usage_new(*(ContainerId*) key);
			g_hash_table_insert(container_manifest, &old_time->cntnr_id,
					old_time);
		}
		old_time->read_size = job_id;
	}

	g_hash_table_iter_init(&iter, cntnr_usg_mntr->dense_map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		ContainerUsage *old_time = g_hash_table_lookup(container_manifest, key);
		if (old_time == NULL) {
			old_time = container_usage_new(*(ContainerId*) key);
			g_hash_table_insert(container_manifest, &old_time->cntnr_id,
					old_time);
		}
		old_time->read_size = job_id;
	}

	/* no jobid is less than 0, so all container are update. */
	update_container_manifest(container_manifest, -1);

	g_hash_table_destroy(container_manifest);
	return 0;
}

/* --------------------------------------------------------------------------*/
/**
 * @Synopsis record all sparse containers in usage file. 
 *
 * @Param cntnr_usg_mntr
 * @Param job_id
 */
/* ----------------------------------------------------------------------------*/
int container_usage_monitor_print(ContainerUsageMonitor *cntnr_usg_mntr,
		int32_t job_id, GHashTable* old_sparse_containers) {
	char path[500];
	strcpy(path, working_path);
	strcat(path, "jobs/");
	char file_name[20];
	sprintf(file_name, "job%d.sparse", job_id);
	strcat(path, file_name);
	GHashTableIter iter;
	gpointer key, value;
	FILE* usage_file;

	if (rewriting_algorithm != CUMULUS) {
		/* CUMULUS write sparse file in deletions */
		usage_file = fopen(path, "w+");
	}
	/* sparse containers */
	int old_sparse_num = 0;
	g_hash_table_iter_init(&iter, cntnr_usg_mntr->sparse_map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		ContainerUsage* cntnr_usg = (ContainerUsage*) value;
		if (old_sparse_containers
				&& g_hash_table_lookup(old_sparse_containers,
						&cntnr_usg->cntnr_id)) {
			old_sparse_num++;
		}
		if (rewriting_algorithm != CUMULUS) {
			fprintf(usage_file, "%d:%d:%.2f\n", cntnr_usg->cntnr_id,
					cntnr_usg->read_size, container_usage_calc(cntnr_usg));
		}
	}
	if (rewriting_algorithm != CUMULUS) {
		if (fclose(usage_file) < 0) {
			/* Error may happen here. */
			perror(strerror(errno));
			return -1;
		}
	}

	/* for cumulus */
	sprintf(file_name, "job%d.usage", job_id);
	strcpy(path, working_path);
	strcat(path, "jobs/");
	strcat(path, file_name);
	usage_file = fopen(path, "w+");
	/* sparse containers */
	g_hash_table_iter_init(&iter, cntnr_usg_mntr->sparse_map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		ContainerUsage* cntnr_usg = (ContainerUsage*) value;

		fprintf(usage_file, "%d:%d:%.2f\n", cntnr_usg->cntnr_id,
				cntnr_usg->read_size, container_usage_calc(cntnr_usg));
	}
	/* dense container */
	g_hash_table_iter_init(&iter, cntnr_usg_mntr->dense_map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		ContainerUsage* cntnr_usg = (ContainerUsage*) value;

		fprintf(usage_file, "%d:%d:%.2f\n", cntnr_usg->cntnr_id,
				cntnr_usg->read_size, container_usage_calc(cntnr_usg));
	}

	if (fclose(usage_file) < 0) {
		/* Error may happen here. */
		perror(strerror(errno));
		return -1;
	}

	container_usage_monitor_update_manifest(cntnr_usg_mntr, job_id);
	return old_sparse_num;
}

/* for cumulus */
GHashTable* load_deleted_containers(int deleted_job, int versions) {
	GHashTable *deleted_containers = g_hash_table_new_full(g_int_hash,
			g_int_equal, NULL, container_usage_free);
	GHashTable *sparse_containers = g_hash_table_new_full(g_int_hash,
			g_int_equal, NULL, container_usage_free);

	char path[500];
	strcpy(path, working_path);
	strcat(path, "jobs/");
	char file_name[20];
	sprintf(file_name, "job%d.usage", deleted_job);
	strcat(path, file_name);
	/* read the usage file of the delete jod */
	FILE* usage_file = fopen(path, "r+");
	if (usage_file) {
		ContainerId id;
		int32_t size;
		float tmp;
		while (fscanf(usage_file, "%d:%d:%f", &id, &size, &tmp) != EOF) {
			ContainerUsage* usage = (ContainerUsage*) malloc(
					sizeof(ContainerUsage));
			usage->cntnr_id = id;
			usage->read_size = size;
			g_hash_table_insert(deleted_containers, &usage->cntnr_id, usage);
		}
		fclose(usage_file);
	}

	int live_container_num = 0;
	int i = 1;
	for (; i <= versions; ++i) {
		strcpy(path, working_path);
		strcat(path, "jobs/");
		char file_name[20];
		sprintf(file_name, "job%d.usage", deleted_job + i);
		strcat(path, file_name);
		usage_file = fopen(path, "r+");

		/* read the old file */
		if (usage_file) {
			ContainerId id;
			int32_t size;
			float tmp;
			while (fscanf(usage_file, "%d:%d:%f", &id, &size, &tmp) != EOF) {

				ContainerUsage* usage = g_hash_table_lookup(sparse_containers,
						&id);
				if (!usage) {
					usage = (ContainerUsage*) malloc(sizeof(ContainerUsage));
					usage->cntnr_id = id;
					usage->read_size = size;
					g_hash_table_replace(sparse_containers, &usage->cntnr_id,
							usage);
					live_container_num++;
				}
				if (usage->read_size < size) {
					usage->read_size = size;
				}

				/* it can not be reclaimed */
				if (g_hash_table_contains(deleted_containers, &id))
					g_hash_table_remove(deleted_containers, &id);
			}
			fclose(usage_file);
		}
	}

	strcpy(path, working_path);
	strcat(path, "jobs/");
	sprintf(file_name, "job%d.sparse", deleted_job + versions);
	strcat(path, file_name);
	usage_file = fopen(path, "w+");

	if (usage_file) {
		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, sparse_containers);
		while (g_hash_table_iter_next(&iter, &key, &value)) {
			ContainerUsage* cntnr_usg = (ContainerUsage*) value;
			if (container_usage_calc(cntnr_usg) < hbr_usage_threshold)
				fprintf(usage_file, "%d:%d:%.2f\n", cntnr_usg->cntnr_id,
						cntnr_usg->read_size, container_usage_calc(cntnr_usg));

		}
		fclose(usage_file);
	}
	g_hash_table_destroy(sparse_containers);

	int delete_container_num = g_hash_table_size(deleted_containers);
	char logfile[] = "delete.log";
	int fd = open(logfile, O_WRONLY | O_CREAT, S_IRWXU);
	lseek(fd, 0, SEEK_END);
	char buf[100];

	/*
	 * number of containers deleted
	 * number of live containers
	 */
	sprintf(buf, "%d %d\n", delete_container_num, live_container_num);
	write(fd, buf, strlen(buf));
	close(fd);

	return deleted_containers;
}

GHashTable* load_historical_sparse_containers(int32_t job_id) {
	if (job_id == 0) {
		return NULL;
	}
	char path[500];
	strcpy(path, working_path);
	strcat(path, "jobs/");
	char file_name[20];
	sprintf(file_name, "job%d.sparse", job_id - 1);
	strcat(path, file_name);
	FILE* usage_file = fopen(path, "r+");

	GHashTable* sparse_containers = g_hash_table_new_full(g_int_hash,
			g_int_equal, NULL, free);

	if (usage_file) {
		ContainerId id;
		int32_t size;
		float tmp;
		while (fscanf(usage_file, "%d:%d:%f", &id, &size, &tmp) != EOF) {
			ContainerUsage* usage = (ContainerUsage*) malloc(
					sizeof(ContainerUsage));
			usage->cntnr_id = id;
			usage->read_size = size;
			g_hash_table_insert(sparse_containers, &usage->cntnr_id, usage);
		}
		fclose(usage_file);
	}
	return sparse_containers;
}

void destroy_historical_sparse_containers(GHashTable* sparse_containers) {
	if (sparse_containers == 0)
		return;
	g_hash_table_destroy(sparse_containers);
}
