/*
 * Many indexes, such as Extreme Binning, SiLo, and Sparse Indexing,
 * need an additional segment phase
 * to consist super-chunk and extract eigenvalue.
 */
#include "global.h"
#include "dedup.h" 
#include "jcr.h"
#include "tools/sync_queue.h"
#include "index/index.h"
#include "storage/cfl_monitor.h"

extern int fingerprint_index_type;
extern BOOL enable_hbr;
extern BOOL enable_cache_filter;
extern int recv_hash(Chunk **chunk);
extern int recv_trace_chunk(Chunk **chunk);
extern CFLMonitor* cfl_monitor;
extern int simulation_level;

/* output of prepare_thread */
static SyncQueue* eigenvalue_queue;
static pthread_t prepare_t;
static GHashTable *sparse_containers;

int sparse_chunk_count = 0;
int64_t sparse_chunk_amount = 0;
int in_cache_count = 0;
int64_t in_cache_amount = 0;

static int segment_num = 0;
static double avg_segment_size = 0;
static double avg_eigenvalue_num = 0;

void send_chunk_with_eigenvalue(Chunk *chunk) {
	sync_queue_push(eigenvalue_queue, chunk);
}

int recv_chunk_with_eigenvalue(Chunk **new_chunk) {
	Chunk *chunk = sync_queue_pop(eigenvalue_queue);
	if (chunk->length == FILE_END) {
		*new_chunk = chunk;
		return FILE_END;
	} else if (chunk->length == STREAM_END) {
		*new_chunk = chunk;
		return STREAM_END;
	}
	chunk->container_id = index_search(&chunk->hash, chunk->eigenvalue);
	if (chunk->container_id != TMP_CONTAINER_ID) {
		chunk->status |= DUPLICATE;
		if (enable_hbr && sparse_containers
				&& g_hash_table_lookup(sparse_containers, &chunk->container_id)
						!= NULL) {
			++sparse_chunk_count;
			sparse_chunk_amount += chunk->length;
			chunk->status |= SPARSE;
		}
		if (!enable_cache_filter
				|| is_container_already_in_cache(cfl_monitor,
						chunk->container_id) == FALSE) {
			chunk->status |= NOT_IN_CACHE;
		} else {
			in_cache_count++;
			in_cache_amount += chunk->length;
		}
	}

	*new_chunk = chunk;
	return SUCCESS;
}

/* 
 * Many fingerprint indexes do not need this process, like ram-index and ddfs.  
 * But others, like extreme binning and SiLo, 
 * need this process to buffer some fingerprints to extract characteristic fingerprint.
 */
/* 
 * Some kinds of fingerprint index needs FILE_END signal, such as extreme binning 
 * */
void * no_segment(void *arg) {
	Jcr *jcr = (Jcr*) arg;
	Recipe *processing_recipe = 0;
	while (TRUE) {
		Chunk *chunk = NULL;
		int signal;
		if (simulation_level == SIMULATION_ALL)
			signal = recv_trace_chunk(&chunk);
		else
			signal = recv_hash(&chunk);

		if (signal == STREAM_END) {
			send_chunk_with_eigenvalue(chunk);
			break;
		}
		if (processing_recipe == 0) {
			processing_recipe = sync_queue_pop(jcr->waiting_files_queue);
			if (simulation_level == SIMULATION_NO)
				/* It is cost */
				puts(processing_recipe->filename);
		}
		if (signal == FILE_END) {
			/* TO-DO */
			if (simulation_level < SIMULATION_ALL)
				close(processing_recipe->fd);
			sync_queue_push(jcr->completed_files_queue, processing_recipe);
			processing_recipe = 0;
			free_chunk(chunk);
			/* FILE notion is meaningless for following threads */
			continue;
		}
		/* TO-DO */
		processing_recipe->chunknum++;
		processing_recipe->filesize += chunk->length;
		send_chunk_with_eigenvalue(chunk);
	}
	return NULL;
}

/* A function pointer.
 * The input of extract_eigenvalue() is as follow:
 * NC, NC, FILE_END, NC, NC, NC, FILE_END,...,NC, FILE_END, STREAM_END
 * (NC is the abbreviation of normal chunk, and FILE_END and STREAM_END are signal.)
 * It returns NULL in most times, which indicates a boundary has not been found.
 * Whenever a non-NULL pointer to EigenValue is return,
 * a segment boundary is found and the eigenvalue of the last segment is in return value.
 * It's important to note that the boundary chunk is belong to the next segment.
 */
EigenValue *(*extract_eigenvalue)(Chunk *);

void *segment_thread(void *arg) {
	static int segment_size = 0;
	Jcr *jcr = (Jcr*) arg;
	Recipe *processing_recipe = 0;
	Queue *segment = queue_new();
	while (TRUE) {
		Chunk *chunk = NULL;
		int signal;
		if (simulation_level == SIMULATION_ALL)
			signal = recv_trace_chunk(&chunk);
		else
			signal = recv_hash(&chunk);

		if (signal != STREAM_END && processing_recipe == 0) {
			processing_recipe = sync_queue_pop(jcr->waiting_files_queue);
			if (simulation_level == SIMULATION_NO)
				puts(processing_recipe->filename);
		}

		/*
		 * Normal chunks and signal.
		 * A STREAM_END signal must be a boundary.
		 */
		EigenValue* eigenvalue = extract_eigenvalue(chunk);
		if (signal == STREAM_END || eigenvalue) {
			/*
			 * We get the eigenvalue,
			 * which indicates a segment boundary is found.
			 *  */
			int size = queue_size(segment);
			if (size > 0 && eigenvalue == NULL) {
				/* It is possible that the stream is end but the eigenvalue is NULL. */
				Chunk *buffered_chunk = queue_top(segment);
				eigenvalue = (EigenValue*) malloc(
						sizeof(EigenValue) + sizeof(Fingerprint));
				memcpy(&eigenvalue->values[0], &buffered_chunk->hash,
						sizeof(Fingerprint));
				eigenvalue->value_num = 1;
			}

			Chunk *buffered_chunk = queue_pop(segment);
			if (buffered_chunk) {
				if (buffered_chunk->data
						== NULL&& simulation_level < SIMULATION_APPEND)
				/* only for extreme binning */
					lseek(processing_recipe->fd, 0, SEEK_SET);
				/* The first chunk in segment */
				buffered_chunk->eigenvalue = (EigenValue*) malloc(
						sizeof(EigenValue)
								+ eigenvalue->value_num * sizeof(Fingerprint));

				memcpy(buffered_chunk->eigenvalue, eigenvalue,
						sizeof(EigenValue)
								+ eigenvalue->value_num * sizeof(Fingerprint));
			}
			while (buffered_chunk) {
				if (buffered_chunk->data == NULL) {
					/*
					 * Some kinds of Fingerprint indexes,
					 * such as extreme binning,
					 * will free the data in extract_eigenvalue function
					 * due to the length of its segments is unlimited.
					 */
					buffered_chunk->data = malloc(buffered_chunk->length);
					if (simulation_level < SIMULATION_APPEND) {

						read(processing_recipe->fd, buffered_chunk->data,
								buffered_chunk->length);
					}
				}
				segment_size += buffered_chunk->length;
				send_chunk_with_eigenvalue(buffered_chunk);
				buffered_chunk = queue_pop(segment);
			}
			if (eigenvalue) {
				avg_segment_size = (avg_segment_size * segment_num
						+ segment_size) / (segment_num + 1);
				avg_eigenvalue_num = (avg_eigenvalue_num * segment_num
						+ eigenvalue->value_num) / (segment_num + 1);
				segment_num++;
				segment_size = 0;
			}

			if (eigenvalue)
				free(eigenvalue);
			eigenvalue = NULL;
		}

		if (signal == STREAM_END) {
			send_chunk_with_eigenvalue(chunk);
			break;
		} else if (signal == FILE_END) {
			if (simulation_level < SIMULATION_ALL)
				close(processing_recipe->fd);
			sync_queue_push(jcr->completed_files_queue, processing_recipe);
			processing_recipe = 0;
			free_chunk(chunk);
			continue;
		}
		queue_push(segment, chunk);

		processing_recipe->chunknum++;
		processing_recipe->filesize += chunk->length;
	}
	queue_free(segment, free_chunk);
	printf(
			"segment_thread is finished:\nsegment_num=%d, avg_segment_size=%.3fMB, avg_eigenvalue_num=%.3f\n",
			segment_num, avg_segment_size / 1024 / 1024, avg_eigenvalue_num);
	return NULL;
}

int start_segment_phase(Jcr *jcr) {
	eigenvalue_queue = sync_queue_new(6000);
	jcr->historical_sparse_containers = load_historical_sparse_containers(
			jcr->job_id);
	sparse_containers = jcr->historical_sparse_containers;
	sparse_chunk_count = 0;
	sparse_chunk_amount = 0;
	switch (fingerprint_index_type) {
	case RAM_INDEX:
	case DDFS_INDEX:
		extract_eigenvalue = NULL;
		pthread_create(&prepare_t, NULL, no_segment, jcr);
		break;
	case EXBIN_INDEX:
		extract_eigenvalue = extract_eigenvalue_exbin;
		pthread_create(&prepare_t, NULL, segment_thread, jcr);
		break;
	case SILO_INDEX:
		extract_eigenvalue = extract_eigenvalue_silo;
		pthread_create(&prepare_t, NULL, segment_thread, jcr);
		break;
	case SPARSE_INDEX:
		extract_eigenvalue = extract_eigenvalue_sparse;
		pthread_create(&prepare_t, NULL, segment_thread, jcr);
		break;
	default:
		dprint("wrong index type!")
		;
		return FALSE;
	}
	return TRUE;
}

void stop_segment_phase() {
	pthread_join(prepare_t, NULL);
	if (sparse_containers)
		destroy_historical_sparse_containers(sparse_containers);
}
