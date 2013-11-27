#include "destor.h"
#include "jcr.h"
#include "tools/rabin_chunking.h"
#include "tools/sync_queue.h"

extern int recv_block(unsigned char **data);

/* chunk queue */
static SyncQueue* chunk_queue;
static pthread_t chunk_t;

/*static int64_t chunk_num = 0;
 static double total_size = 0;*/

static void send_chunk(unsigned char *data, int32_t size) {
	int s = size > 0 ? : size - size;
	struct chunk* ck = new_chunk(s);

	memcpy(ck->data, data, size);
	ck->size = size;

	sync_queue_push(chunk_queue, ck);
}

static void term_chunk_queue() {
	sync_queue_term(chunk_queue);
}

int recv_chunk(struct chunk **ck) {
	*ck = sync_queue_pop(chunk_queue);
	if (*ck == NULL) {
		return STREAM_END;
	}
	if ((*ck)->size < 0)
		return FILE_END;
	return (*ck)->size;
}

static void* chunk_thread(void *arg) {
	int leftlen = 0;
	int left_offset = 0;
	unsigned char leftbuf[DEFAULT_BLOCK_SIZE + destor.chunk_max_size];
	int block_size = 0;

	char zeros[destor.chunk_max_size];
	bzero(zeros, destor.chunk_max_size);
	unsigned char data[destor.chunk_max_size];

	while (1) {
		unsigned char *block;
		if (block_size >= 0 && leftlen < destor.chunk_max_size) {

			block_size = recv_block(&block);

			/* save this signal */
			if (block_size > 0) {
				memmove(leftbuf, leftbuf + left_offset, leftlen);
				left_offset = 0;
				memcpy(leftbuf + leftlen, block, block_size);
				leftlen += block_size;
			}
			free(block);
		}

		if (leftlen > 0) {
			TIMER_DECLARE(b, e);
			TIMER_BEGIN(b);

			int chunk_size = 0;
			if (destor.chunk_algorithm == CHUNK_RABIN
					|| destor.chunk_algorithm == CHUNK_NORMALIZED_RABIN)
				chunk_size = rabin_chunk_data(leftbuf + left_offset, leftlen);
			else
				chunk_size =
						destor.chunk_avg_size > leftlen ?
								leftlen : destor.chunk_avg_size;

			TIMER_END(jcr->chunk_time, b, e);

			memcpy(data, leftbuf + left_offset, chunk_size);
			leftlen -= chunk_size;
			left_offset += chunk_size;

			if (memcmp(zeros, data, chunk_size) == 0) {
				jcr->zero_chunk_num++;
				jcr->zero_chunk_size += chunk_size;
			}

			send_chunk(data, chunk_size);
		} else {
			if (size == FILE_END) {
				leftlen = 0;
				left_offset = 0;
				size = 0;
				windows_reset();
				/* block is the file name */
				send_chunk(block, -(strlen(block) + 1));
			} else if (size == STREAM_END) {
				term_chunk_queue();
				break;
			}
		}
	}

	return NULL;
}

void start_chunk_phase() {
	chunkAlg_init();
	chunk_queue = sync_queue_new(100);
	pthread_create(&chunk_t, NULL, chunk_thread, NULL);
}

void stop_chunk_phase() {
	pthread_join(chunk_t, NULL);
}
