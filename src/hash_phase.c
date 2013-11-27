#include "destor.h"
#include "jcr.h"
#include "tools/sync_queue.h"

extern int recv_chunk(struct chunk** ck);

/* hash queue */
static SyncQueue* hash_queue;
static pthread_t hash_t;

static void send_hashed_chunk(struct chunk* ck) {
	sync_queue_push(hash_queue, ck);
}

static void term_hash_queue() {
	sync_queue_term(hash_queue);
}

int recv_hashed_chunk(struct chunk **nck) {
	*nck = sync_queue_pop(hash_queue);
	if (*nck == NULL) {
		return STREAM_END;
	}
	if ((*nck)->size < 0)
		return FILE_END;
	return (*nck)->size;
}

static void* sha1_thread(void* arg) {
	while (1) {
		struct chunk* ck = NULL;
		int signal = recv_chunk(&ck);
		if (signal == STREAM_END) {
			term_hash_queue();
			break;
		} else if (signal == FILE_END) {
			send_hashed_chunk(ck);
			continue;
		}

		TIMER_DECLARE(b, e);
		TIMER_BEGIN(b);
		SHA_CTX ctx;
		SHA_Init(&ctx);
		SHA_Update(&ctx, ck->data, ck->size);
		SHA_Final(&ck->fp, &ctx);
		TIMER_END(jcr.hash_time, b, e);

		send_hash(ck);
	}
	return NULL;
}

void start_hash_phase() {
	hash_queue = sync_queue_new(100);
	pthread_create(&hash_t, NULL, sha1_thread, NULL);
}

void stop_hash_phase() {
	pthread_join(hash_t, NULL);
}
