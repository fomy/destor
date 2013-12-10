#include "destor.h"
#include "jcr.h"
#include "backup.h"

static pthread_t hash_t;

static void* sha1_thread(void* arg) {
	while (1) {
		struct chunk* c = sync_queue_pop(chunk_queue);

		if (c == NULL) {
			sync_queue_term(hash_queue);
			break;
		}

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			sync_queue_push(hash_queue, c);
			continue;
		}

		TIMER_DECLARE(b, e);
		TIMER_BEGIN(b);
		SHA_CTX ctx;
		SHA_Init(&ctx);
		SHA_Update(&ctx, c->data, c->size);
		SHA_Final(c->fp, &ctx);
		TIMER_END(jcr.hash_time, b, e);

		sync_queue_push(hash_queue, c);
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
