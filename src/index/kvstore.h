#ifndef KVSTORE_H_
#define KVSTORE_H_

#include "../destor.h"

void init_kvstore();

extern void (*close_kvstore)();
extern int64_t* (*kvstore_lookup)(char *key);
extern void (*kvstore_update)(char *key, int64_t id);
extern void (*kvstore_delete)(char* key, int64_t id);

#endif
