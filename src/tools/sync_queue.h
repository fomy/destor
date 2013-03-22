#ifndef SYNC_QUEUE_H_
#define SYNC_QUEUE_H_

#include "../global.h"
#include "../tools/queue.h"

typedef struct {
    Queue *queue;
    int max_size;/* the max size of queue */
    pthread_mutex_t mutex;
    pthread_cond_t max_work;
    pthread_cond_t min_work;
}SyncQueue;

SyncQueue* sync_queue_new(int);
void sync_queue_free(SyncQueue*, void (*)(void*));
void sync_queue_push(SyncQueue*, void*);
void* sync_queue_pop(SyncQueue*);
int sync_queue_size(SyncQueue* s_queue);

#endif
