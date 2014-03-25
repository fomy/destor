#include "kvstore.h"

extern void init_kvstore_htable();
extern void close_kvstore_htable();
extern int64_t* kvstore_htable_lookup(char* key);
extern void kvstore_htable_update(char* key, int64_t id);

/*
 * Mapping a fingerprint (or feature) to the prefetching unit.
 */

int32_t kvpair_size;

/*
 * Create a new kv pair.
 */
kvpair new_kvpair_full(char* key){
    kvpair kvp = malloc(kvpair_size);
    memcpy(get_key(kvp), key, destor.index_key_size);
    int64_t* values = get_value(kvp);
    int i;
    for(i = 0; i<destor.index_value_length; i++){
    	memset(&values[i], TEMPORARY_ID, sizeof(int64_t));
    }
    return kvp;
}

kvpair new_kvpair(){
	 kvpair kvp = malloc(kvpair_size);
	 int64_t* values = get_value(kvp);
	 int i;
	 for(i = 0; i<destor.index_value_length; i++){
		 memset(&values[i], TEMPORARY_ID, sizeof(int64_t));
	 }
	 return kvp;
}

void kv_update(kvpair kv, int64_t id){
    int64_t* value = get_value(kv);
	memmove(&value[1], value,
			(destor.index_value_length - 1) * sizeof(int64_t));
	value[0] = id;
}

void free_kvpair(kvpair kvp){
	free(kvp);
}

void (*close_kvstore)();
int64_t* (*kvstore_lookup)(char *key);
void (*kvstore_update)(char *key, int64_t id);

void init_kvstore() {
    kvpair_size = destor.index_key_size + destor.index_value_length * 8;

    switch(destor.index_key_value_store){
    	case INDEX_KEY_VALUE_HTABLE:
    		init_kvstore_htable();

    		close_kvstore = close_kvstore_htable;
    		kvstore_lookup = kvstore_htable_lookup;
    		kvstore_update = kvstore_htable_update;

    		break;
    	default:
    		WARNING("Invalid key-value store!");
    		exit(1);
    }
}
