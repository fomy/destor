/*
 * delete_server.c
 *
 *  Created on: Jun 21, 2012
 *      Author: fumin
 */
#include "destor.h"
#include "storage/containerstore.h"
#include "recipe/recipestore.h"
#include "index/index.h"
#include "cma.h"

/* A simple wrap.
 * Just to make the interfaces of the index module more consistent.
 */
static inline void delete_an_entry(fingerprint *fp, int64_t *id){
	index_delete(fp, *id);
}

/*
 * We assume a FIFO order of deleting backup, namely the oldest backup is deleted first.
 */
void do_delete(int jobid) {

	GHashTable *invalid_containers = trunc_manifest(jobid);

	init_index();
	init_recipe_store();

	/* Delete the invalid entries in the key-value store */
	if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY){
		init_container_store();

		struct backupVersion* bv = open_backup_version(jobid);

		/* The entries pointing to Invalid Containers are invalid. */
		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, invalid_containers);
		while(g_hash_table_iter_next(&iter, &key, &value)){
			containerid id = *(containerid*)key;
			NOTICE("Reclaim container %lld", id);
			struct containerMeta* cm = retrieve_container_meta_by_id(id);

			container_meta_foreach(cm, delete_an_entry, &id);

			free_container_meta(cm);
		}

		bv->deleted = 1;
		update_backup_version(bv);
		free_backup_version(bv);

		close_container_store();
	}else if(destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY){
		/* Ideally, the entries pointing to segments in backup versions of a 'bv_num' less than 'jobid' are invalid. */
		/* (For simplicity) Since a FIFO order is given, we only need to remove the IDs exactly matched 'bv_num'. */
		struct backupVersion* bv = open_backup_version(jobid);

		struct segmentRecipe* sr;
		while((sr=read_next_segment(bv))){
			segment_recipe_foreach(sr, delete_an_entry, &sr->id);
		}

		bv->deleted = 1;
		update_backup_version(bv);
		free_backup_version(bv);

	}else{
		WARNING("Invalid index type");
		exit(1);
	}

	close_recipe_store();
	close_index();

	char logfile[] = "delete.log";
	FILE *fp = fopen(logfile, "a");
	/*
	 * ID of the job we delete,
	 * number of live containers,
	 * memory footprint
	 */
	fprintf(fp, "%d %d %d\n",
			jobid,
			destor.live_container_num,
			destor.index_memory_footprint);

	fclose(fp);

	/* record the IDs of invalid containers */
	sds didfilepath = sdsdup(destor.working_directory);
	char s[128];
	sprintf(s, "recipes/delete_%d.id", jobid);
	didfilepath = sdscat(didfilepath, s);

	FILE*  didfile = fopen(didfilepath, "w");
	if(didfile){
		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, invalid_containers);
		while(g_hash_table_iter_next(&iter, &key, &value)){
			containerid id = *(containerid*)key;
			fprintf(didfile, "%lld\n", id);
		}

		fclose(didfile);
	}


	g_hash_table_destroy(invalid_containers);
}
