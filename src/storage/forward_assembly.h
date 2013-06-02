#ifndef ASSEMBLY_H_
#define ASSEMBLY_H_
#include "cfl_monitor.h"
#include "../job/jobmanage.h"
#include "../dedup.h"

void init_assembly_area(int read_cache_size, JobVolume *jvol, int64_t number);
CFLMonitor* destroy_assembly_area();
Chunk* asm_get_chunk();

#endif
