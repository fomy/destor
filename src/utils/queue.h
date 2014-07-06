/*
 * queue.h
 *
 *  Created on: May 21, 2012
 *      Author: fumin
 */

#ifndef QUEUE_H_
#define QUEUE_H_

typedef struct queue_ele_tag {
	struct queue_ele_tag *next;
	void *data;
} queue_ele_t;

/*
 * Structure describing a queue
 */
typedef struct queue_tag {
	queue_ele_t *first, *last; /* work queue */
	int elem_num;
	//int max_elem_num; //-1 means infi.
} Queue;

Queue* queue_new();
void queue_free(Queue *queue, void (*)(void*));
void queue_push(Queue *queue, void *element);
void* queue_pop(Queue *queue);
int queue_size(Queue *queue);
void queue_foreach(Queue *queue, void (*func)(void *data, void *user_data),
		void *user_data);
void* queue_get_n(Queue *queue, int n);
void * queue_top(Queue *queue);
void* queue_find(Queue* queue, int (*hit)(void*, void*), void* data);

#endif /* QUEUE_H_ */
