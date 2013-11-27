/*
 * optimal_restore.c
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */
#include "destor.h"
#include "jcr.h"
#include "recipe/recipemanage.h"
#include "storage/container_manage.h"

void init_optimal_cache(){

}

void* optimal_restore_thread(void *arg) {
	init_optimal_cache();

	int i = 0, k;
	struct chunkPointer *cp;
	while (i < jcr.bv->number_of_chunks) {
		struct recipe *r = read_next_n_chunk_pointers(jcr.bv, 1, &cp, &k);
		if (r) {
			send_restore_chunk(r->filename, -(sdslen(r->filename) + 1));
			free_recipe(r);
		} else {

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
				send_restore_chunk(c->data, c->size);
				free_chunk(c);
			}

		}
	}

	term_restore_queue();

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
	/*	sprintf(buf, "%d %d %d %.2f %.4f %.2f\n", jcr->job_id, monitor->ocf,
	 monitor->ccf, get_cfl(monitor),
	 1.0 * jcr->job_size / (monitor->ccf * 1024.0 * 1024),
	 (double) jcr->job_size * 1000000
	 / (jcr->read_chunk_time * 1024 * 1024));*/
	write(fd, buf, strlen(buf));
	close(fd);

	return NULL;
}
