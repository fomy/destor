#include "global.h"
#include "dedup.h" 
#include "jcr.h"
#include "tools/rabin_chunking.h"
#include "tools/sync_queue.h"

extern int recv_data(DataBuffer** data_buffer);
extern int chunking_algorithm;

/* chunk_size must be a power of 2 */
uint32_t chunk_size = 8192;
uint32_t max_chunk_size = 65536;
uint32_t min_chunk_size = 1024;

/* chunk queue */
static SyncQueue* chunk_queue;
static pthread_t chunk_t;

static int64_t chunk_num = 0;
static double total_size = 0;

static void send_chunk(Chunk* chunk) {
	sync_queue_push(chunk_queue, chunk);
}

static void signal_chunk(int signal) {
	Chunk *chunk = allocate_chunk();
	chunk->length = signal;
	sync_queue_push(chunk_queue, chunk);
}

int recv_chunk(Chunk **new_chunk) {
	Chunk *chunk = sync_queue_pop(chunk_queue);
	if (chunk->length == FILE_END) {
		/*free_chunk(chunk);*/
		*new_chunk = chunk;
		return FILE_END;
	} else if (chunk->length == STREAM_END) {
		/*free_chunk(chunk);*/
		*new_chunk = chunk;
		return STREAM_END;
	}
	*new_chunk = chunk;
	return SUCCESS;
}

static void* rabin_chunk_thread(void *arg) {
	int leftlen = 0;
	int left_offset = 0;
	unsigned char leftbuf[READ_BUFFER_SIZE + max_chunk_size];
	int signal = 0;

	Jcr *jcr = (Jcr*) arg;

	char zeros[max_chunk_size];
	bzero(zeros, max_chunk_size);
	while (TRUE) {
		Chunk *new_chunk = allocate_chunk();

		if (signal >= 0 && leftlen < max_chunk_size) {
			DataBuffer *data_buffer = NULL;
			signal = recv_data(&data_buffer);
			/* save this signal */
			if (signal == SUCCESS) {
				memmove(leftbuf, leftbuf + left_offset, leftlen);
				left_offset = 0;
				memcpy(leftbuf + leftlen, data_buffer->data, data_buffer->size);
				leftlen += data_buffer->size;
			}
			free(data_buffer);
		}

		if (leftlen > 0) {
			TIMER_DECLARE(b, e);
			TIMER_BEGIN(b);

			if (chunking_algorithm == RABIN_CHUNK
					|| chunking_algorithm == NRABIN_CHUNK)
				new_chunk->length = rabin_chunk_data(leftbuf + left_offset,
						leftlen);
			else if (chunking_algorithm == FIXED_CHUNK)
				new_chunk->length = chunk_size > leftlen ? leftlen : chunk_size;
			else
				dprint("Unknown chunking algorithm!");

			chunk_num++;
			total_size += new_chunk->length;

			TIMER_END(jcr->chunk_time, b, e);

			new_chunk->data = (unsigned char*) malloc(new_chunk->length);
			memcpy(new_chunk->data, leftbuf + left_offset, new_chunk->length);
			leftlen -= new_chunk->length;
			left_offset += new_chunk->length;

			if (memcmp(zeros, new_chunk->data, new_chunk->length) == 0) {
				jcr->zero_chunk_count++;
				jcr->zero_chunk_amount += new_chunk->length;
			}

			send_chunk(new_chunk);
		} else {
			if (signal == FILE_END) {
				leftlen = 0;
				left_offset = 0;
				signal = 0;
				windows_reset();
				free(new_chunk);
				signal_chunk(FILE_END);
			} else if (signal == STREAM_END) {
				free(new_chunk);
				signal_chunk(STREAM_END);
				break;
			}
		}
	}
	printf("chunk phase is finished:\nchunk_num=%ld, avg_chunk_size=%.3f\n",
			chunk_num, total_size / chunk_num);
	return NULL ;
}

int start_chunk_phase(Jcr* jcr) {
	chunkAlg_init();
	chunk_queue = sync_queue_new(100);
	pthread_create(&chunk_t, NULL, rabin_chunk_thread, jcr);
}

void stop_chunk_phase() {
	pthread_join(chunk_t, NULL );
}
