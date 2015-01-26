#include "destor.h"
#include "jcr.h"
#include "chunking/chunking.h"
#include "backup.h"
#include "storage/containerstore.h"

static pthread_t chunk_t;
static int64_t chunk_num;

static int (*chunking)(unsigned char* buf, int size);

static inline int fixed_chunk_data(unsigned char* buf, int size){
	return destor.chunk_avg_size > size ? size : destor.chunk_avg_size;
}

/*
 * chunk-level deduplication.
 * Destor currently supports fixed-sized chunking and (normalized) rabin-based chunking.
 */
static void* chunk_thread(void *arg) {
	int leftlen = 0;
	int leftoff = 0;
	unsigned char *leftbuf = malloc(DEFAULT_BLOCK_SIZE + destor.chunk_max_size);

	unsigned char *zeros = malloc(destor.chunk_max_size);
	bzero(zeros, destor.chunk_max_size);
	unsigned char *data = malloc(destor.chunk_max_size);

	struct chunk* c = NULL;

	while (1) {

		/* Try to receive a CHUNK_FILE_START. */
		c = sync_queue_pop(read_queue);

		if (c == NULL) {
			sync_queue_term(chunk_queue);
			break;
		}

		assert(CHECK_CHUNK(c, CHUNK_FILE_START));
		sync_queue_push(chunk_queue, c);

		/* Try to receive normal chunks. */
		c = sync_queue_pop(read_queue);
		if (!CHECK_CHUNK(c, CHUNK_FILE_END)) {
			memcpy(leftbuf, c->data, c->size);
			leftlen += c->size;
			free_chunk(c);
			c = NULL;
		}

		while (1) {
			/* c == NULL indicates more data for this file can be read. */
			while ((leftlen < destor.chunk_max_size) && c == NULL) {
				c = sync_queue_pop(read_queue);
				if (!CHECK_CHUNK(c, CHUNK_FILE_END)) {
					memmove(leftbuf, leftbuf + leftoff, leftlen);
					leftoff = 0;
					memcpy(leftbuf + leftlen, c->data, c->size);
					leftlen += c->size;
					free_chunk(c);
					c = NULL;
				}
			}

			if (leftlen == 0) {
				assert(c);
				break;
			}

			TIMER_DECLARE(1);
			TIMER_BEGIN(1);

			int	chunk_size = chunking(leftbuf + leftoff, leftlen);

			TIMER_END(1, jcr.chunk_time);

			struct chunk *nc = new_chunk(chunk_size);
			memcpy(nc->data, leftbuf + leftoff, chunk_size);
			leftlen -= chunk_size;
			leftoff += chunk_size;

			if (memcmp(zeros, nc->data, chunk_size) == 0) {
				VERBOSE("Chunk phase: %ldth chunk  of %d zero bytes",
						chunk_num++, chunk_size);
				jcr.zero_chunk_num++;
				jcr.zero_chunk_size += chunk_size;
			} else
				VERBOSE("Chunk phase: %ldth chunk of %d bytes", chunk_num++,
						chunk_size);

			sync_queue_push(chunk_queue, nc);
		}

		sync_queue_push(chunk_queue, c);
		leftoff = 0;
		c = NULL;

		if(destor.chunk_algorithm == CHUNK_RABIN ||
				destor.chunk_algorithm == CHUNK_NORMALIZED_RABIN)
			windows_reset();

	}

	free(leftbuf);
	free(zeros);
	free(data);
	return NULL;
}



void start_chunk_phase() {

	if (destor.chunk_algorithm == CHUNK_RABIN){
		int pwr;
		for (pwr = 0; destor.chunk_avg_size; pwr++) {
			destor.chunk_avg_size >>= 1;
		}
		destor.chunk_avg_size = 1 << (pwr - 1);

		assert(destor.chunk_avg_size >= destor.chunk_min_size);
		assert(destor.chunk_avg_size <= destor.chunk_max_size);
		assert(destor.chunk_max_size <= CONTAINER_SIZE - CONTAINER_META_SIZE);

		chunkAlg_init();
		chunking = rabin_chunk_data;
	}else if(destor.chunk_algorithm == CHUNK_NORMALIZED_RABIN){
		int pwr;
		for (pwr = 0; destor.chunk_avg_size; pwr++) {
			destor.chunk_avg_size >>= 1;
		}
		destor.chunk_avg_size = 1 << (pwr - 1);

		assert(destor.chunk_avg_size >= destor.chunk_min_size);
		assert(destor.chunk_avg_size <= destor.chunk_max_size);
		assert(destor.chunk_max_size <= CONTAINER_SIZE - CONTAINER_META_SIZE);

		chunkAlg_init();
		chunking = normalized_rabin_chunk_data;
	}else if(destor.chunk_algorithm == CHUNK_TTTD){
		int pwr;
		for (pwr = 0; destor.chunk_avg_size; pwr++) {
			destor.chunk_avg_size >>= 1;
		}
		destor.chunk_avg_size = 1 << (pwr - 1);

		assert(destor.chunk_avg_size >= destor.chunk_min_size);
		assert(destor.chunk_avg_size <= destor.chunk_max_size);
		assert(destor.chunk_max_size <= CONTAINER_SIZE - CONTAINER_META_SIZE);

		chunkAlg_init();
		chunking = tttd_chunk_data;
	}else if(destor.chunk_algorithm == CHUNK_FIXED){
		assert(destor.chunk_avg_size <= CONTAINER_SIZE - CONTAINER_META_SIZE);

		destor.chunk_max_size = destor.chunk_avg_size;
		chunking = fixed_chunk_data;
	}else if(destor.chunk_algorithm == CHUNK_FILE){
		/*
		 * approximate file-level deduplication
		 * It splits the stream according to file boundaries.
		 * For a larger file, we need to split it due to container size limit.
		 * Hence, our approximate file-level deduplication are only for files smaller than CONTAINER_SIZE - CONTAINER_META_SIZE.
		 * Similar to fixed-sized chunking of $(( CONTAINER_SIZE - CONTAINER_META_SIZE )) chunk size.
		 * */
		destor.chunk_avg_size = CONTAINER_SIZE - CONTAINER_META_SIZE;
		destor.chunk_max_size = CONTAINER_SIZE - CONTAINER_META_SIZE;
		chunking = fixed_chunk_data;
	}else{
		NOTICE("Invalid chunking algorithm");
		exit(1);
	}

	chunk_queue = sync_queue_new(100);
	pthread_create(&chunk_t, NULL, chunk_thread, NULL);
}

void stop_chunk_phase() {
	pthread_join(chunk_t, NULL);
	NOTICE("chunk phase stops successfully!");
}
