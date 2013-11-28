#include "destor.h"
#include "jcr.h"
#include "recipe/recipemanage.h"
#include "storage/container_manage.h"
#include "tools/sync_queue.h"

static SyncQueue *restore_chunk_queue;
static SyncQueue *restore_recipe_queue;

void send_restore_chunk(struct chunk* c) {
	sync_queue_push(restore_chunk_queue, c);
}

void term_restore_chunk_queue() {
	sync_queue_term(restore_chunk_queue);
}

int recv_restore_chunk(struct chunk **ck) {
	*ck = sync_queue_pop(restore_chunk_queue);
	if (*ck == NULL) {
		return STREAM_END;
	}
	if ((*ck)->size < 0)
		return FILE_END;
	return (*ck)->size;
}

/* May be a recipe or a chunkPointer. */
void send_restore_recipe(void* cp) {
	sync_queue_push(restore_recipe_queue, cp);
}

void term_restore_recipe_queue() {
	sync_queue_term(restore_recipe_queue);
}

int recv_restore_recipe(void **cp) {
	*cp = sync_queue_pop(restore_recipe_queue);
	if (*cp == NULL) {
		return STREAM_END;
	}
	return 1;
}

static void* lru_restore_thread(void *arg) {
	struct lruCache *cache;
	if (destor.simulation_level >= SIMULATION_RESTORE)
		cache = new_lru_cache(destor.restore_cache[1], free_container_meta,
				lookup_fingerprint_in_container_meta);
	else
		cache = new_lru_cache(destor.restore_cache[1], free_container,
				lookup_fingerprint_in_container);

	struct recipe* r = NULL;
	struct chunkPointer* cp = NULL;

	int sig = recv_restore_recipe(&r);
	while (sig != STREAM_END) {

		struct chunk *c = new_chunk(sdslen(r->filename) + 1);
		strcpy(c->data, r->filename);
		c->size = -(sdslen(r->filename) + 1);
		send_restore_chunk(c);

		int num = r->chunknum, i;
		for (i = 0; i < num; i++) {
			sig = recv_restore_recipe(&cp);

			if (destor.simulation_level >= SIMULATION_RESTORE) {
				struct containerMeta *cm = lru_cache_lookup(cache, &cp->fp);
				if (!cm) {
					cm = retrieve_container_meta_by_id(cp->id);
					assert(lookup_fingerprint_in_container_meta(cm, &cp->fp));
					lru_cache_insert(cache, cm);
				}

			} else {
				struct container *con = lru_cache_lookup(cache, &cp->fp);
				if (!con) {
					con = retrieve_container_by_id(cp->id);
					lru_cache_insert(cache, con);
				}
				struct chunk *c = get_chunk_in_container(con, &cp->fp);
				assert(c);
				send_restore_chunk(c);
			}
		}

		free_recipe(r);
		sig = recv_restore_recipe(&r);
	}

	term_restore_chunk_queue();

	/*	char logfile[40];
	 strcpy(logfile, "restore.log");
	 int fd = open(logfile, O_WRONLY | O_CREAT, S_IRWXU);
	 lseek(fd, 0, SEEK_END);
	 char buf[100];

	 * job id,
	 * minimal container number,
	 * actually read container number,
	 * CFL,
	 * speed factor
	 * throughput

	 sprintf(buf, "%d %d %d %.2f %.4f %.2f\n", jcr->job_id, monitor->ocf,
	 monitor->ccf, get_cfl(monitor),
	 1.0 * jcr->job_size / (monitor->ccf * 1024.0 * 1024),
	 (double) jcr->job_size * 1000000
	 / (jcr->read_chunk_time * 1024 * 1024));
	 write(fd, buf, strlen(buf));
	 close(fd);*/

	return NULL;
}

static void* read_recipe_thread(void *arg) {
	int k;
	struct chunkPointer *cp;
	struct recipe *r = read_next_n_chunk_pointers(jcr.bv, 10, &cp, &k);
	while (r != NULL || k != 0) {
		if (r) {
			send_restore_recipe(r);
		} else {
			int i;
			for (i = 0; i < k; i++) {
				struct chunkPointer* p = (struct chunkPointer*) malloc(
						sizeof(struct chunkPointer));
				memcpy(p, &cp[i], sizeof(struct chunkPointer));
				send_restore_recipe(p);
			}

			free(cp);
		}
		r = read_next_n_chunk_pointers(jcr.bv, 10, &cp, &k);
	}

	term_restore_recipe_queue();
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
	while (1) {
		int sig = recv_restore_chunk(&c);

		if (!c)
			break;

		if (c->size < 0) {
			destor_log(DESTOR_NOTICE, c->data);

			sds filepath = strdup(jcr.path);
			filepath = sdscat(filepath, c->data);

			int len = sdslen(jcr->path);
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

			if (simulation_level == SIMULATION_NO) {
				if (fp)
					fclose(fp);
				fp = fopen(filepath, "w+");
			}
		} else {
			assert(simulation_level == SIMULATION_NO);
			fwrite(c->data, c->size, 1, fp);
		}

		free_chunk(c);

	}

	if (fp)
		fclose(fp);
}

void do_restore(int revision, char *path) {

	init_restore_jcr(revision, path);

	printf("job id: %d\n", jcr.id);
	printf("backup path: %s\n", jcr.bv->path);

	if (!path)
		jcr->path = sdscpy(jcr->path, jcr.bv->path);

	restore_chunk_queue = sync_queue_new(100);
	restore_recipe_queue = sync_queue_new(100);

	pthread_t recipe_t, read_t;

	pthread_create(&recipe_t, NULL, read_recipe_thread, NULL);

	if (destor.restore_cache[0] == RESTORE_CACHE_LRU) {
		destor_log(DESTOR_NOTICE, "restore cache is LRU");
		pthread_create(&read_t, NULL, lru_restore_thread, NULL);
	} else if (destor.restore_cache[0] == RESTORE_CACHE_OPT) {

	} else if (destor.restore_cache[0] == RESTORE_CACHE_ASM) {

	} else {
		fprintf(stderr, "Invalid restore cache.\n");
		exit(1);
	}

	write_restore_data();

	assert(sync_queue_size(restore_chunk_queue) == 0);
	assert(sync_queue_size(restore_recipe_queue) == 0);

	free_backup_version(jcr.bv);

}

