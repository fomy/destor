#include "destor.h"
#include "jcr.h"
#include "recipe/recipestore.h"
#include "storage/containerstore.h"
#include "tools/lru_cache.h"
#include "restore.h"

static void* lru_restore_thread(void *arg) {
	struct lruCache *cache;
	if (destor.simulation_level >= SIMULATION_RESTORE)
		cache = new_lru_cache(destor.restore_cache[1], free_container_meta,
				lookup_fingerprint_in_container_meta);
	else
		cache = new_lru_cache(destor.restore_cache[1], free_container,
				lookup_fingerprint_in_container);

	struct chunk* c;
	while ((c = sync_queue_pop(restore_recipe_queue))) {

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			sync_queue_push(restore_chunk_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (destor.simulation_level >= SIMULATION_RESTORE) {
			struct containerMeta *cm = lru_cache_lookup(cache, &c->fp);
			if (!cm) {
				cm = retrieve_container_meta_by_id(c->id);
				assert(lookup_fingerprint_in_container_meta(cm, &c->fp));
				lru_cache_insert(cache, cm, NULL, NULL);
				jcr.read_container_num++;
			}

			TIMER_END(1, jcr.read_chunk_time);
		} else {
			struct container *con = lru_cache_lookup(cache, &c->fp);
			if (!con) {
				con = retrieve_container_by_id(c->id);
				lru_cache_insert(cache, con, NULL, NULL);
				jcr.read_container_num++;
			}
			struct chunk *rc = get_chunk_in_container(con, &c->fp);
			assert(rc);
			TIMER_END(1, jcr.read_chunk_time);
			sync_queue_push(restore_chunk_queue, rc);
		}

		free_chunk(c);
	}

	sync_queue_term(restore_chunk_queue);

	return NULL;
}

static void* read_recipe_thread(void *arg) {

	int i, j, k;
	for (i = 0; i < jcr.bv->number_of_files; i++) {
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		struct recipe *r = read_next_recipe_meta(jcr.bv);

		struct chunk *c = new_chunk(sdslen(r->filename) + 1);
		strcpy(c->data, r->filename);
		SET_CHUNK(c, CHUNK_FILE_START);

		TIMER_END(1, jcr.read_recipe_time);

		sync_queue_push(restore_recipe_queue, c);

		jcr.file_num++;

		for (j = 0; j < r->chunknum; j++) {
			TIMER_DECLARE(1);
			TIMER_BEGIN(1);

			struct chunkPointer* cp = read_next_n_chunk_pointers(jcr.bv, 1, &k);

			struct chunk* c = new_chunk(0);
			memcpy(&c->fp, &cp->fp, sizeof(fingerprint));
			c->size = cp->size;
			c->id = cp->id;

			TIMER_END(1, jcr.read_recipe_time);
			jcr.data_size += c->size;
			jcr.chunk_num++;

			sync_queue_push(restore_recipe_queue, c);
			free(cp);
		}

		c = new_chunk(0);
		SET_CHUNK(c, CHUNK_FILE_END);
		sync_queue_push(restore_recipe_queue, c);

		free_recipe(r);
	}

	sync_queue_term(restore_recipe_queue);
	return NULL;
}

void write_restore_data() {

	char *p, *q;
	q = jcr.path + 1;/* ignore the first char*/
	/*
	 * recursively make directory
	 */
	while ((p = strchr(q, '/'))) {
		if (*p == *(p - 1)) {
			q++;
			continue;
		}
		*p = 0;
		if (access(jcr.path, 0) != 0) {
			mkdir(jcr.path, S_IRWXU | S_IRWXG | S_IRWXO);
		}
		*p = '/';
		q = p + 1;
	}

	struct chunk *c = NULL;
	FILE *fp = NULL;

	while ((c = sync_queue_pop(restore_chunk_queue))) {

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (CHECK_CHUNK(c, CHUNK_FILE_START)) {
			destor_log(DESTOR_NOTICE, "%s", c->data);

			sds filepath = sdsdup(jcr.path);
			filepath = sdscat(filepath, c->data);

			int len = sdslen(jcr.path);
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

			if (destor.simulation_level == SIMULATION_NO) {
				assert(fp == NULL);
				fp = fopen(filepath, "w");
			}

		} else if (CHECK_CHUNK(c, CHUNK_FILE_END)) {
			if (fp)
				fclose(fp);
			fp = NULL;
		} else {
			assert(destor.simulation_level == SIMULATION_NO);
			fwrite(c->data, c->size, 1, fp);
		}

		free_chunk(c);

		TIMER_END(1, jcr.write_chunk_time);
	}

}

void do_restore(int revision, char *path) {

	init_restore_jcr(revision, path);

	destor_log(DESTOR_NOTICE, "job id: %d", jcr.id);
	destor_log(DESTOR_NOTICE, "backup path: %s", jcr.bv->path);
	destor_log(DESTOR_NOTICE, "restore to: %s", jcr.path);

	restore_chunk_queue = sync_queue_new(100);
	restore_recipe_queue = sync_queue_new(100);

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	puts("==== restore begin ====");

	pthread_t recipe_t, read_t;
	pthread_create(&recipe_t, NULL, read_recipe_thread, NULL);

	if (destor.restore_cache[0] == RESTORE_CACHE_LRU) {
		destor_log(DESTOR_NOTICE, "restore cache is LRU");
		pthread_create(&read_t, NULL, lru_restore_thread, NULL);
	} else if (destor.restore_cache[0] == RESTORE_CACHE_OPT) {
		destor_log(DESTOR_NOTICE, "restore cache is OPT");
		pthread_create(&read_t, NULL, optimal_restore_thread, NULL);
	} else if (destor.restore_cache[0] == RESTORE_CACHE_ASM) {
		destor_log(DESTOR_NOTICE, "restore cache is ASM");
		pthread_create(&read_t, NULL, assembly_restore_thread, NULL);
	} else {
		fprintf(stderr, "Invalid restore cache.\n");
		exit(1);
	}

	write_restore_data();

	assert(sync_queue_size(restore_chunk_queue) == 0);
	assert(sync_queue_size(restore_recipe_queue) == 0);

	free_backup_version(jcr.bv);

	TIMER_END(1, jcr.total_time);
	puts("==== restore end ====");

	printf("job id: %d\n", jcr.id);
	printf("restore path: %s\n", jcr.path);
	printf("number of files: %d\n", jcr.file_num);
	printf("number of chunks: %d\n", jcr.chunk_num);
	printf("total size: %ld\n", jcr.data_size);
	printf("total time: %.3fs\n", jcr.total_time / 1000000);
	printf("throughput: %.2fMB/s\n",
			(double) jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));
	printf("speed factor: %.2f\n",
			(double) jcr.data_size / (1024 * 1024 * jcr.read_container_num));

	printf("read_recipe_time : %.3fs, %.2fMB/s\n",
			jcr.read_recipe_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_recipe_time / 1024 / 1024);
	printf("read_chunk_time : %.3fs, %.2fMB/s\n", jcr.read_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_chunk_time / 1024 / 1024);
	printf("write_chunk_time : %.3fs, %.2fMB/s\n",
			jcr.write_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.write_chunk_time / 1024 / 1024);

	char logfile[] = "restore.log";
	FILE *fp = fopen(logfile, "a");

	/*
	 * job id,
	 * chunk num,
	 * data size,
	 * actually read container number,
	 * speed factor,
	 * throughput
	 */
	fprintf(fp, "%d %lld %d %.4f %.4f\n", jcr.id, jcr.data_size,
			jcr.read_container_num,
			(double) jcr.data_size / (1024 * 1024 * jcr.read_container_num),
			(double) jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));

	fclose(fp);
}

