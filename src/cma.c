/*
 * The Container-Marker Algorithm.
 *  After each backup, we read the original manifest, and update the backup times.
 *  In each deletion operation, 
 *  the containers with a time smaller than the time of deleted backup are reclaimed.
 */

#include "cma.h"
#include "storage/containerstore.h"
#include "jcr.h"

struct record{
    containerid id;
    int time;
};

void update_manifest(GHashTable *monitor){

    GHashTable *manifest = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);
    
    sds fname = sdsdup(destor.working_directory);
    fname = sdscat(fname, "/manifest");
    FILE *fp = NULL;
    if((fp = fopen(fname, "r"))){
    	/* file exists. Reconstruct the manifest from the file. */
    	struct record tmp;
    	while(fscanf(fp, "%lld,%d", &tmp.id, &tmp.time) == 2){
    		struct record* rec = (struct record*) malloc(sizeof(struct record));
    		rec->id = tmp.id;
    		rec->time = tmp.time;
    		g_hash_table_insert(manifest, &rec->id, rec);
    	}

        NOTICE("CMA: read %d records.", g_hash_table_size(manifest));

        fclose(fp);
    }

    /* Update the backup times in the manifest. */
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, monitor);
    while(g_hash_table_iter_next(&iter, &key, &value)){
        /* the key is a pointer to a container ID. */
        struct record *r = g_hash_table_lookup(manifest, key);
        if(!r){
            r = (struct record*) malloc(sizeof(struct record));
            r->id = *(containerid*)key;
            g_hash_table_insert(manifest, &r->id, r);
        }
        r->time = jcr.id;
    }

    /* Flush the manifest */
    if((fp = fopen(fname, "w"))){
    	/* Update the manifest into the file. */
    	g_hash_table_iter_init(&iter, manifest);
    	while(g_hash_table_iter_next(&iter, &key, &value)){
    		struct record* r = value;
    		fprintf(fp, "%lld,%d\n", r->id, r->time);
    	}

        NOTICE("CMA: update %d records.", g_hash_table_size(manifest));
        fclose(fp);
    }else{
    	WARNING("Cannot create the manifest!");
    	exit(1);
    }

    destor.live_container_num = g_hash_table_size(manifest);

    g_hash_table_destroy(manifest);

    sdsfree(fname);
}

/*
 * Be called when users delete backups in FIFO order.
 * Delete all backups earlier than jobid.
 * All container IDs with a time smaller than or equal to jobid can be removed.
 * Return these IDs.
 */
GHashTable* trunc_manifest(int jobid){
	/* The containers we reclaim */
	GHashTable *invalid_containers = g_hash_table_new_full(g_int64_hash, g_int64_equal, free, NULL);

    GHashTable *manifest = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);

    sds fname = sdsdup(destor.working_directory);
    fname = sdscat(fname, "/manifest");
    FILE *fp = NULL;
    if((fp = fopen(fname, "r"))){
    	/* file exists. Reconstruct the manifest from the file. */
    	struct record tmp;
    	while(fscanf(fp, "%lld,%d", &tmp.id, &tmp.time) == 2){
    		struct record* rec = (struct record*) malloc(sizeof(struct record));
    		if(tmp.time <= jobid){
    			/* This record can be removed. */
    			containerid *cid = (containerid*) malloc(sizeof(containerid));
    			*cid = tmp.id;
    			g_hash_table_insert(invalid_containers, cid, NULL);
                NOTICE("CMA: container %lld can be reclaimed.", cid);
    		}else{
    			/* This record remains valid. */
    			rec->id = tmp.id;
    			rec->time = tmp.time;
    			g_hash_table_insert(manifest, &rec->id, rec);
    		}
    	}

        NOTICE("CMA: %d of records are valid.", g_hash_table_size(manifest));
        NOTICE("CMA: %d of records are going to be reclaimed.", g_hash_table_size(invalid_containers));

        fclose(fp);
    }else{
    	NOTICE("manifest doesn't exist!");
    	exit(1);
    }

    if((fp = fopen(fname, "w"))){
    	GHashTableIter iter;
    	gpointer key, value;
    	g_hash_table_iter_init(&iter, manifest);
    	while(g_hash_table_iter_next(&iter, &key, &value)){
    		struct record* rec = value;
    		fprintf(fp, "%lld,%d\n", rec->id, rec->time);
    	}
    	fclose(fp);
    }else{
    	WARNING("CMA: cannot create manifest!");
    	exit(1);
    }

    destor.live_container_num = g_hash_table_size(manifest);

    g_hash_table_destroy(manifest);

	return invalid_containers;
}
