#include "global.h"
#include "dedup.h" 
#include "jcr.h"
#include "tools/rabin_chunking.h"
#include "tools/sync_queue.h"

/* chunk_size must be a power of 2 */
uint32_t chunk_size = 8192;
uint32_t max_chunk_size = 65526;
uint32_t min_chunk_size = 2048;

/* output of read_thread */
extern SyncQueue* read_queue;

/* chunk queue */
extern SyncQueue* chunk_queue;

void* rabin_chunk(void *arg) {
    int leftlen = 0;
    int left_offset = 0;
    unsigned char leftbuf[READ_BUFFER_SIZE + max_chunk_size];
    int signal = 0;

    Jcr *jcr = (Jcr*) arg;

    while (TRUE) {
        Chunk *new_chunk = (Chunk*) malloc(sizeof(Chunk));
        new_chunk->duplicate = FALSE;
        new_chunk->container_id = TMP_CONTAINER_ID;
        new_chunk->length = 0;
        new_chunk->data = 0;

        if (signal >= 0 && leftlen < max_chunk_size) {
            DataBuffer *data_buffer = sync_queue_pop(read_queue);
            /* save this signal */
            signal = data_buffer->size;
            if (signal > 0) {
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

            new_chunk->length = chunk_data(leftbuf + left_offset, leftlen);

            TIMER_END(jcr->chunk_time, b, e);

            new_chunk->data = (unsigned char*) malloc(new_chunk->length);
            memcpy(new_chunk->data, leftbuf + left_offset, new_chunk->length);
            leftlen -= new_chunk->length;
            left_offset += new_chunk->length;
            sync_queue_push(chunk_queue, new_chunk);
        } else {
            if (signal == FILE_END) {
                leftlen = 0;
                left_offset = 0;
                signal = 0;
                windows_reset();
                new_chunk->length = FILE_END;
                sync_queue_push(chunk_queue, new_chunk);
            } else if(signal == STREAM_END) {
                new_chunk->length = STREAM_END;
                new_chunk->data = 0;
                sync_queue_push(chunk_queue, new_chunk);
                break;
            }
        }
    }
    return NULL;
}

void* fixed_chunk(void *arg){
    int leftlen = 0;
    int left_offset = 0;
    unsigned char leftbuf[READ_BUFFER_SIZE + chunk_size];
    int signal = 0;

    Jcr *jcr = (Jcr*) arg;

    while (TRUE) {
        Chunk *new_chunk = (Chunk*) malloc(sizeof(Chunk));
        new_chunk->duplicate = FALSE;
        new_chunk->container_id = TMP_CONTAINER_ID;
        new_chunk->length = 0;
        new_chunk->data = 0;

        if (signal >= 0 && leftlen < chunk_size) {
            DataBuffer *data_buffer = sync_queue_pop(read_queue);
            /* save this signal */
            signal = data_buffer->size;
            if (signal > 0) {
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

            /*new_chunk->length = chunk_data(leftbuf + left_offset, leftlen);*/
            new_chunk->length = chunk_size > leftlen ? leftlen : chunk_size;

            TIMER_END(jcr->chunk_time, b, e);

            new_chunk->data = (unsigned char*) malloc(new_chunk->length);
            memcpy(new_chunk->data, leftbuf + left_offset, new_chunk->length);
            leftlen -= new_chunk->length;
            left_offset += new_chunk->length;
            sync_queue_push(chunk_queue, new_chunk);
        } else {
            if (signal == FILE_END) {
                leftlen = 0;
                left_offset = 0;
                signal = 0;
                new_chunk->length = FILE_END;
                sync_queue_push(chunk_queue, new_chunk);
            } else if(signal == STREAM_END) {
                new_chunk->length = STREAM_END;
                new_chunk->data = 0;
                sync_queue_push(chunk_queue, new_chunk);
                break;
            }
        }
    }
    return NULL;
}
