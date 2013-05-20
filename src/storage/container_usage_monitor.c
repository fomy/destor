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
	FILE* usage_file = fopen(path, "w+");
	GHashTableIter iter;
	gpointer key, value;
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
		fprintf(usage_file, "%d:%d:%.2f\n", cntnr_usg->cntnr_id,
				cntnr_usg->read_size, container_usage_calc(cntnr_usg));
	}
	if (fclose(usage_file) < 0) {
		/* Error may happen here. */
		perror(strerror(errno));
		return -1;
	}
	return old_sparse_num;
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
