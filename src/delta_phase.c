/*
 * delta_phase.c
 *
 *  Created on: Jul 28, 2014
 *      Author: fumin
 */
#include "destor.h"
#include "jcr.h"
#include "backup.h"

static pthread_t delta_t;

struct base_chunk_pointer {
	fingerprint fp;
	containerid id;
};

static GHashTable *delta_index;
static pthread_mutex_t mutex;

containerid lookup_delta_index(fingerprint *fp){
	pthread_mutex_lock(&mutex);
	struct base_chunk_pointer* p = g_hash_table_lookup(delta_index, fp);
	if(p){
		pthread_mutex_unlock(&mutex);
		return p->id;
	}
	pthread_mutex_unlock(&mutex);
	NOTICE("Fail delta: The base chunk has not been written to a container yet!");
	return TEMPORARY_ID;
}

void update_delta_index(fingerprint *fp, containerid id){

	struct base_chunk_pointer* p = (struct base_chunk_pointer*)
			malloc(sizeof(struct base_chunk_pointer));
	p->id = id;
	memcpy(&p->fp, fp, sizeof(fingerprint));

	pthread_mutex_lock(&mutex);
	g_hash_table_replace(delta_index, &p->fp, p);
	pthread_mutex_unlock(&mutex);
}

void init_delta_index(){
    delta_index = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free);

	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/delta.index");

	/* Initialize the feature index from the dump file. */
	FILE *fp;
	if ((fp = fopen(indexpath, "r"))) {
		/* The number of features */
		int key_num;
		fread(&key_num, sizeof(int), 1, fp);
		for (; key_num > 0; key_num--) {
			/* Read a feature */
			struct base_chunk_pointer* p = (struct base_chunk_pointer*)
					malloc(sizeof(struct base_chunk_pointer));
			fread(&p->fp, sizeof(fingerprint), 1, fp);
			fread(&p->id, sizeof(containerid), 1, fp);

			g_hash_table_insert(delta_index, &p->fp, p);
		}
		fclose(fp);
	}

	sdsfree(indexpath);

    if (pthread_mutex_init(&mutex, 0)) {
   		puts("Failed to init delta mutex!");
    	exit(1);
   	}
}

void close_delta_index(){
	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/delta.index");

	FILE *fp;
	if ((fp = fopen(indexpath, "w")) == NULL) {
		perror("Can not open delta.index for write because:");
		exit(1);
	}

	int key_num = g_hash_table_size(delta_index);
	fwrite(&key_num, sizeof(int), 1, fp);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, delta_index);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct base_chunk_pointer* p = value;
		fwrite(&p->fp, sizeof(fingerprint), 1, fp);
		fwrite(&p->id, sizeof(containerid), 1, fp);
	}
	fclose(fp);

	g_hash_table_destroy(delta_index);
}

void *delta_thread(void *arg) {
    while (1) {
        struct chunk* c = sync_queue_pop(rewrite_queue);

        if (c == NULL)
            /* backup job finish */
            break;

        if(CHECK_CHUNK(c, CHUNK_FILE_START) ||
        		CHECK_CHUNK(c, CHUNK_FILE_END) ||
        		CHECK_CHUNK(c, CHUNK_SEGMENT_START) ||
        		CHECK_CHUNK(c, CHUNK_SEGMENT_END)){
        	sync_queue_push(delta_queue, c);
        	continue;
        }

        if(CHECK_CHUNK(c, CHUNK_DUPLICATE)){
        	sync_queue_push(delta_queue, c);
        	continue;
        }

        /* do delta compression (simulation) */
        if(c->delta != NULL){
        	c->delta->baseid = lookup_delta_index(&c->delta->basefp);
        	if(c->delta->baseid == TEMPORARY_ID){
        		free_delta(c->delta);
        		c->delta = NULL;
        	}
        }

        sync_queue_push(delta_queue, c);
    }

    sync_queue_term(delta_queue);

	return NULL;
}

void start_delta_phase() {
    delta_queue = sync_queue_new(1000);

    init_delta_index();

    pthread_create(&delta_t, NULL, delta_thread, NULL);

}

void stop_delta_phase() {
    pthread_join(delta_t, NULL);
}
