/*
 * kvstore_htable.c
 *
 *  Created on: Mar 23, 2014
 *      Author: fumin
 */

#include "../destor.h"

typedef char* kvpair;

#define get_key(kv) (kv)
#define get_value(kv) ((int64_t*)(kv+destor.index_key_size))

static GHashTable *htable;

static gboolean g_key_equal(char* a, char* b){
	return !memcmp(a, b, destor.index_key_size);
}

static int32_t kvpair_size;

/*
 * Create a new kv pair.
 */
static kvpair new_kvpair_full(char* key){
    kvpair kvp = malloc(kvpair_size);
    memcpy(get_key(kvp), key, destor.index_key_size);
    int64_t* values = get_value(kvp);
    int i;
    for(i = 0; i<destor.index_value_length; i++){
    	values[i] = TEMPORARY_ID;
    }
    return kvp;
}

static kvpair new_kvpair(){
	 kvpair kvp = malloc(kvpair_size);
	 int64_t* values = get_value(kvp);
	 int i;
	 for(i = 0; i<destor.index_value_length; i++){
		 values[i] = TEMPORARY_ID;
	 }
	 return kvp;
}

static void kv_update(kvpair kv, int64_t id){
    int64_t* value = get_value(kv);
	memmove(&value[1], value,
			(destor.index_value_length - 1) * sizeof(int64_t));
	value[0] = id;
}

static void free_kvpair(kvpair kvp){
	free(kvp);
}

void init_kvstore_htable(){
    kvpair_size = destor.index_key_size + destor.index_value_length * 8;

	htable = g_hash_table_new_full(g_int64_hash, g_key_equal,
			free_kvpair, NULL);

	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/htable");

	/* Initialize the feature index from the dump file. */
	FILE *fp;
	if ((fp = fopen(indexpath, "r"))) {
		/* The number of features */
		int feature_num;
		fread(&feature_num, sizeof(int), 1, fp);
		for (; feature_num > 0; feature_num--) {
			/* Read a feature */
			kvpair kv = new_kvpair();
			fread(get_key(kv), destor.index_key_size, 1, fp);

			/* The number of segments/containers the feature refers to. */
			int id_num, i;
			fread(&id_num, sizeof(int), 1, fp);
			assert(id_num <= destor.index_value_length);

			for (i = 0; i < id_num; i++)
				/* Read an ID */
				fread(&get_value(kv)[i], sizeof(int64_t), 1, fp);

			g_hash_table_insert(htable, get_key(kv), kv);
		}
		fclose(fp);
	}

	sdsfree(indexpath);
}

void close_kvstore_htable() {
	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/htable");

	FILE *fp;
	if ((fp = fopen(indexpath, "w")) == NULL) {
		perror("Can not open index/htable for write because:");
		exit(1);
	}

	int feature_num = g_hash_table_size(htable);
	fwrite(&feature_num, sizeof(int), 1, fp);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, htable);
	while (g_hash_table_iter_next(&iter, &key, &value)) {

		/* Write a feature. */
		kvpair kv = value;
		fwrite(get_key(kv), destor.index_key_size, 1, fp);

		/* Write the number of segments/containers */
		fwrite(&destor.index_value_length, sizeof(int), 1, fp);
		int i;
		for (i = 0; i < destor.index_value_length; i++)
			fwrite(&get_value(kv)[i], sizeof(int64_t), 1, fp);

	}

	/* It is not accurate */
	destor.index_memory_footprint = g_hash_table_size(htable)
			* (destor.index_key_size + sizeof(int64_t) * destor.index_value_length);

	fclose(fp);

	sdsfree(indexpath);

	g_hash_table_destroy(htable);
}

/*
 * For top-k selection method.
 */
int64_t* kvstore_htable_lookup(char* key) {
	kvpair kv = g_hash_table_lookup(htable, key);
	return kv ? get_value(kv) : NULL;
}

void kvstore_htable_update(char* key, int64_t id) {
	kvpair kv = g_hash_table_lookup(htable, key);
	if (!kv) {
		kv = new_kvpair_full(key);
		g_hash_table_insert(htable, get_key(kv), kv);
	}
	kv_update(kv, id);
}
