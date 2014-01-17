#ifndef SYNC_QUEUE_H_
#define SYNC_QUEUE_H_

#include <stdlib.h>
#include <pthread.h>
#include "queue.h"

typedef struct {
	int term; // terminated
	int max_size;/* the max size of queue */
	Queue *queue;
	pthread_mutex_t mutex;
	pthread_cond_t max_work;
	pthread_cond_t min_work;
} SyncQueue;

SyncQueue* sync_queue_new(int);
void sync_queue_free(SyncQueue*, void (*)(void*));
void sync_queue_push(SyncQueue*, void*);
void* sync_queue_pop(SyncQueue*);
void sync_queue_term(SyncQueue*);
int sync_queue_size(SyncQueue* s_queue);
void* sync_queue_find(SyncQueue* s_queue, int (*hit)(void*, void*), void* data,
		void* (*dup)(void*));
void* sync_queue_get_top(SyncQueue* s_queue);

#endif
