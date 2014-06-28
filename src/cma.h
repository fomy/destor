#ifndef CMA_H_
#define CMA_H_

#include "destor.h"

void update_manifest(GHashTable *monitor);
GHashTable* trunc_manifest(int jobid);

#endif
