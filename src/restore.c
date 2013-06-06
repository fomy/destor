/*
 * restore.c
 *
 *  Created on: Jun 4, 2012
 *      Author: fumin
 */

#include "global.h"
#include "jcr.h"
#include "job/jobmanage.h"
#include "storage/protos.h"
#include "index/index.h"
#include "storage/cfl_monitor.h"
#include "tools/sync_queue.h"
#include "statistic.h"

static int restore_one_file(Jcr*, Recipe *psRp);
static void* read_chunk_thread(void *arg);
int restore(Jcr *jcr);

extern int read_cache_size;
extern BOOL enable_data_cache;
extern int read_cache_type;
extern int optimal_cache_window_size;
extern int simulation_level;
extern DestorStat *destor_stat;

static SyncQueue *recovery_queue;

double read_container_time = 0;

int restore_server(int revision, char *restore_path) {
	Jcr *jcr = new_read_jcr(read_cache_size, enable_data_cache);

	int jobId = revision;

	if ((jcr->job_volume = open_job_volume(jobId)) == NULL) {
		printf("Doesn't exist such job!\n");
		return FAILURE;
	}

	if (simulation_level <= SIMULATION_RECOVERY
			&& destor_stat->simulation_level >= SIMULATION_APPEND
			|| simulation_level >= SIMULATION_APPEND
					&& destor_stat->simulation_level <= SIMULATION_RECOVERY) {
		dprint(
				"the current simulation level is not matched with the destor stat!");
		return FAILURE;
	}

	jcr->job_id = jcr->job_volume->job.job_id;
	jcr->file_num = jcr->job_volume->job.file_num;
	jcr->chunk_num = jcr->job_volume->job.chunk_num;
	strcpy(jcr->backup_path, jcr->job_volume->job.backup_path);

	printf("job id: %d\n", jcr->job_id);
	printf("backup path: %s\n", jcr->job_volume->job.backup_path);

	if (restore_path) {
		strcpy(jcr->restore_path, restore_path);
		strcat(jcr->restore_path, "/");
	} else
		strcpy(jcr->restore_path, jcr->backup_path);

	TIMER_DECLARE(b, e);
	TIMER_BEGIN(b);

	pthread_t read_t;
	recovery_queue = sync_queue_new(100);
	pthread_create(&read_t, NULL, read_chunk_thread, jcr);
	if (restore(jcr) != 0) {
		close_job_volume(jcr->job_volume);
		free_jcr(jcr);
		return FAILURE;
	}
	pthread_join(read_t, NULL);

	if (sync_queue_size(recovery_queue) != 0)
		dprint("recovery_queue is not empty");
	sync_queue_free(recovery_queue, free_chunk);

	TIMER_END(jcr->time, b, e);

	printf("the size of restore job: %ld\n", jcr->job_size);
	printf("elapsed time: %.3fs\n", jcr->time / 1000000);
	printf("throughput: %.2fMB/s\n",
			(double) jcr->job_size * 1000000 / (1024 * 1024 * jcr->time));

	printf("read_cache_efficiency: %.3fs, %.2fMB/s\n", jcr->read_chunk_time / 1000000,
			(double) jcr->job_size * 1000000
					/ (jcr->read_chunk_time * 1024 * 1024));
	printf("read_container_time: %.3fs, %.2fMB/s\n", read_container_time / 1000000,
			(double) jcr->job_size * 1000000
					/ (read_container_time * 1024 * 1024));
	printf("write_file_time: %.3fs, %.2fMB/s\n", jcr->write_file_time / 1000000,
			(double) jcr->job_size * 1000000
					/ (jcr->write_file_time * 1024 * 1024));

	close_job_volume(jcr->job_volume);

	free_jcr(jcr);
	return 0;
}

int restore(Jcr *jcr) {

	char *p, *q;
	q = jcr->restore_path + 1;/* ignore the first char*/
	/*
	 * recursively make directory
	 */
	while ((p = strchr(q, '/'))) {
		if (*p == *(p - 1)) {
			q++;
			continue;
		}
		*p = 0;
		if (access(jcr->restore_path, 0) != 0) {
			mkdir(jcr->restore_path, S_IRWXU | S_IRWXG | S_IRWXO);
		}
		*p = '/';
		q = p + 1;
	}

	if (jcr->file_num >= 1) {
		int number_of_files = jcr->file_num;

		while (number_of_files) {
			Recipe *recipe = read_next_recipe(jcr->job_volume);
			if (restore_one_file(jcr, recipe) != 0)
				printf("failed to restore %s\n", recipe->filename);
			number_of_files--;
			recipe_free(recipe);
		}

	} else {
		puts("No file need to be restored!");
	}
	return SUCCESS;
}

static int restore_one_file(Jcr *jcr, Recipe *recipe) {
	char *filepath = (char*) malloc(
			strlen(jcr->restore_path) + strlen(recipe->filename) + 1);
	strcpy(filepath, jcr->restore_path);
	strcat(filepath, recipe->filename);

	int len = strlen(jcr->restore_path);
	char *q = filepath + len;
	char *p;
	while ((p = strchr(q, '/'))) {
		if (*p == *(p - 1)) {
			q++;
			continue;
		}
		*p = 0;
		if (access(filepath, 0) != 0) {
			mkdir(filepath, S_IRWXU | S_IRWXG | S_IRWXO);
		}
		*p = '/';
		q = p + 1;
	}
	if (simulation_level == SIMULATION_NO)
		puts(filepath);
	int fd;
	if (simulation_level == SIMULATION_NO)
		fd = open(filepath, O_CREAT | O_TRUNC | O_WRONLY,
				S_IRWXU | S_IRWXG | S_IRWXO);
	/*
	 * retrieve file data
	 * jcr->time = 0;
	 */
	while (recipe->filesize) {
		Chunk* chunk = sync_queue_pop(recovery_queue);

		TIMER_DECLARE(b1, e1);
		TIMER_BEGIN(b1);
		/* cherish your disk */
		if (simulation_level == SIMULATION_NO)
			write(fd, chunk->data, chunk->length);
		jcr->job_size += chunk->length;
		TIMER_END(jcr->write_file_time, b1, e1);

		recipe->filesize -= chunk->length;

		free_chunk(chunk);
	}

	if (simulation_level == SIMULATION_NO)
		close(fd);

	return SUCCESS;
}

static CFLMonitor *monitor;
static int64_t chunk_num;

static void* read_chunk_thread(void *arg) {
	Jcr *jcr = (Jcr*) arg;
	monitor = 0;
	if (read_cache_type == LRU_CACHE) {
		puts("cache=LRU");
		jcr->read_cache = container_cache_new(jcr->read_cache_size,
				jcr->enable_data_cache, jcr->job_id);
	} else if (read_cache_type == OPT_CACHE) {
		puts("cache=OPT");
		jcr->read_opt_cache = optimal_container_cache_new(jcr->read_cache_size,
				jcr->enable_data_cache, jcr->job_volume->job_seed_file,
				optimal_cache_window_size);
	} else if (read_cache_type == ASM_CACHE) {
		puts("cache=ASM");
		init_assembly_area(jcr->read_cache_size, jcr->job_volume,
				jcr->chunk_num);
	}
	chunk_num = 0;
	printf("cache_size=%d\n", read_cache_size);

	while (chunk_num < jcr->chunk_num) {
		Chunk *result = 0;

		TIMER_DECLARE(b1, e1);
		TIMER_BEGIN(b1);
		if (read_cache_type == LRU_CACHE) {
			FingerChunk* fchunk = jvol_read_fingerchunk(jcr->job_volume);
			result = container_cache_get_chunk(jcr->read_cache,
					&fchunk->fingerprint, fchunk->container_id);
			free(fchunk);
		} else if (read_cache_type == OPT_CACHE) {
			FingerChunk* fchunk = jvol_read_fingerchunk(jcr->job_volume);
			result = optimal_container_cache_get_chunk(jcr->read_opt_cache,
					&fchunk->fingerprint, fchunk->container_id);
			free(fchunk);
		} else if (read_cache_type == ASM_CACHE) {
			result = asm_get_chunk();
		}
		TIMER_END(jcr->read_chunk_time, b1, e1);

		sync_queue_push(recovery_queue, result);

		chunk_num++;
	}

	if (read_cache_type == LRU_CACHE)
		monitor = container_cache_free(jcr->read_cache);
	else if (read_cache_type == OPT_CACHE)
		monitor = optimal_container_cache_free(jcr->read_opt_cache);
	else if (read_cache_type == ASM_CACHE) {
		monitor = destroy_assembly_area();
	}

	char logfile[40];
	strcpy(logfile, "restore.log");
	int fd = open(logfile, O_WRONLY | O_CREAT, S_IRWXU);
	lseek(fd, 0, SEEK_END);
	char buf[100];
	/*
	 * job id,
	 * minimal container number,
	 * actually read container number,
	 * CFL,
	 * speed factor
	 * throughput
	 */
	sprintf(buf, "%d %d %d %.2f %.4f %.2f\n", jcr->job_id, monitor->ocf,
			monitor->ccf, get_cfl(monitor),
			1.0 * jcr->job_size / (monitor->ccf * 1024.0 * 1024),
			(double) jcr->job_size * 1000000
					/ (jcr->read_chunk_time * 1024 * 1024));
	write(fd, buf, strlen(buf));
	close(fd);

	print_cfl(monitor);
	cfl_monitor_free(monitor);

	return NULL;
}
