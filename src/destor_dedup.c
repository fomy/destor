/*
 * destor_dedup.c
 *  
 *  Created on: Dec 11, 2012
 *      Author: fumin
 */

#include "global.h"
#include "dedup.h"
#include "jcr.h"
#include "index/index.h"
#include "tools/pipeline.h"
#include "storage/cfl_monitor.h"

extern double container_usage_threshold;
/* input */
/* hash queue */
extern PipelineQueue* hash_queue;

/* output */
/* container queue */
extern PipelineQueue* container_queue;
/* finger chunk queue */
extern PipelineQueue *fingerchunk_queue;
