#ifndef KVSTORE_H_
#define KVSTORE_H_

#include "../destor.h"

typedef char* kvpair;

extern int32_t kvpair_size;

#define get_key(kv) (kv)
#define get_value(kv) ((int64_t*)(kv+destor.index_key_size))

kvpair new_kvpair_full(char* key);
kvpair new_kvpair();
void kv_update(kvpair kv, int64_t id);
void free_kvpair(kvpair kvp);

void init_kvstore();

extern void (*close_kvstore)();
extern int64_t* (*kvstore_lookup)(char *key);
extern void (*kvstore_update)(char *key, int64_t id);

#endif
