#include "global.h"
#include "dedup.h" 
#include "jcr.h"
#include "index/index.h"
#include "storage/protos.h"

extern int rewriting_algorithm;

extern int recv_chunk_with_eigenvalue(Chunk **chunk);
extern ContainerId save_chunk(Chunk* chunk);

static pthread_t filter_t;

extern void send_fc_signal();
extern void send_fingerchunk(FingerChunk *fchunk, EigenValue* eigenvalue,
		BOOL update);

extern void* cfl_filter(void* arg);
extern void* cbr_filter(void* arg);
extern void* cap_filter(void* arg);

static void* simply_filter(void* arg) {
	Jcr* jcr = (Jcr*) arg;
	while (TRUE) {
		Chunk* chunk = NULL;
		int signal = recv_chunk_with_eigenvalue(&chunk);

		TIMER_DECLARE(b1, e1);
		TIMER_BEGIN(b1);
		if (signal == STREAM_END) {
			free_chunk(chunk);
			break;
		}

		/* init FingerChunk */
		FingerChunk *new_fchunk = (FingerChunk*) malloc(sizeof(FingerChunk));
		new_fchunk->length = chunk->length;
		memcpy(&new_fchunk->fingerprint, &chunk->hash, sizeof(Fingerprint));
		new_fchunk->container_id = chunk->container_id;

		BOOL update = FALSE;
		if (chunk->status & DUPLICATE) {
			if (chunk->status & SPARSE) {
				/* this chunk is in a sparse container */
				/* rewrite it */
				new_fchunk->container_id = save_chunk(chunk);

				update = TRUE;
				jcr->rewritten_chunk_count++;
				jcr->rewritten_chunk_amount += new_fchunk->length;
			} else {
				jcr->dedup_size += chunk->length;
				++jcr->number_of_dup_chunks;
			}
		} else {
			new_fchunk->container_id = save_chunk(chunk);
			update = TRUE;
		}

		TIMER_END(jcr->filter_time, b1, e1);
		send_fingerchunk(new_fchunk, chunk->eigenvalue, update);
		free_chunk(chunk);
	} //while(TRUE) end

	save_chunk(NULL );

	send_fc_signal();

	return NULL ;
}

int start_filter_phase(Jcr *jcr) {
	if (rewriting_algorithm == NO_REWRITING) {
		puts("rewriting_algorithm=NO");
		pthread_create(&filter_t, NULL, simply_filter, jcr);
	} else if (rewriting_algorithm == CFL_REWRITING) {
		puts("rewriting_algorithm=CFL");
		pthread_create(&filter_t, NULL, cfl_filter, jcr);
	} else if (rewriting_algorithm == CBR_REWRITING) {
		puts("rewriting_algorithm=CBR");
		pthread_create(&filter_t, NULL, cbr_filter, jcr);
	} else if (rewriting_algorithm == CAP_REWRITING) {
		puts("rewriting_algorithm=CAP");
		pthread_create(&filter_t, NULL, cap_filter, jcr);
	} else if (rewriting_algorithm == ECAP_REWRITING) {
		puts("rewriting_algorithm=ECAP");
		pthread_create(&filter_t, NULL, cap_filter, jcr);
	} else {
		dprint("invalid rewriting algorithm\n");
		return FAILURE;
	}
}

void stop_filter_phase() {
	pthread_join(filter_t, NULL );
}
