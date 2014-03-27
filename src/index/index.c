#include "index.h"
#include "kvstore.h"
#include "fingerprint_cache.h"
#include "segmentstore.h"

/* The buffer size > 2 * destor.rewrite_buffer_size */
/* All fingerprints that have been looked up in the index
 * but not been updated. */
struct {
	/* map a fingerprint to a queue of indexElem */
	/* Index all fingerprints in the index buffer. */
	GHashTable *buffered_fingerprints;
	/* The number of buffered chunks */
	int num;
} index_buffer;

extern void init_segmenting_method();
extern void init_sampling_method();

void init_index() {
	/* Do NOT assign a free function for value. */
	index_buffer.buffered_fingerprints = g_hash_table_new_full(g_int64_hash,
			g_fingerprint_equal, NULL, NULL);
	index_buffer.num = 0;

	if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY){
		destor.index_segment_algorithm[0] = INDEX_SEGMENT_FIXED;
		if(destor.index_category[0] == INDEX_CATEGORY_EXACT){
			destor.index_sampling_method[0] = INDEX_SAMPLING_UNIFORM;
			destor.index_sampling_method[1] = 1;
		}
	}else{
		init_segment_management();
	}

	init_sampling_method();
	init_segmenting_method();

	init_kvstore();

	init_fingerprint_cache();

}

void close_index() {
	if(destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY){
		close_segment_management();
	}
	close_kvstore();
}

extern struct{
	/* accessed in dedup phase */
	struct container *container_buffer;
	/* In order to facilitate sampling in container,
	 * we keep a queue for chunks in container buffer. */
	GQueue *chunks;
} storage_buffer;

static void index_lookup_base(struct segment *s){
	int len = g_queue_get_length(s->chunks), i;

	for (i = 0; i < len; ++i) {
		struct chunk* c = g_queue_peek_nth(s->chunks, i);

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
			continue;

		/* First check it in the storage buffer */
		if(storage_buffer.container_buffer
				&& lookup_fingerprint_in_container(storage_buffer.container_buffer, &c->fp)){
			c->id = get_container_id(storage_buffer.container_buffer);
			SET_CHUNK(c, CHUNK_DUPLICATE);
		}

		/*
		 * First check the buffered fingerprints,
		 * recently backup fingerprints.
		 */
		GQueue *tq = g_hash_table_lookup(index_buffer.buffered_fingerprints, &c->fp);
		if (!tq) {
			tq = g_queue_new();
		} else if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			struct indexElem *be = g_queue_peek_head(tq);
			c->id = be->id;
			SET_CHUNK(c, CHUNK_DUPLICATE);
		}

		/* Check the fingerprint cache */
		if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			/* Searching in fingerprint cache */
			int64_t id = fingerprint_cache_lookup(&c->fp);
			if(id != TEMPORARY_ID){
				c->id = id;
				SET_CHUNK(c, CHUNK_DUPLICATE);
			}
		}

		if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			/* Searching in key-value store */
			int64_t* ids = kvstore_lookup((char*)&c->fp);
			if(ids){
				int j;
				for(j = 0;j<destor.index_value_length; j++){
					if(ids[j] == TEMPORARY_ID)
						break;
					/* prefetch the target unit */
					fingerprint_cache_prefetch(ids[j]);
					int64_t id = fingerprint_cache_lookup(&c->fp);
					if(id != TEMPORARY_ID){
						/*
						 * It can be not cached,
						 * since a partial key is possible in near-exact deduplication.
						 */
						c->id = id;
						SET_CHUNK(c, CHUNK_DUPLICATE);
					}else{
						NOTICE("Filter phase: A key collision occurs");
					}
				}
			}else{
				VERBOSE("Dedup phase: non-existing fingerprint");
			}
		}

		/* Insert it into the index buffer */
		struct indexElem *ne = (struct indexElem*) malloc(sizeof(struct indexElem));
		ne->id = c->id;
		memcpy(&ne->fp, &c->fp, sizeof(fingerprint));

		g_queue_push_tail(tq, ne);
		g_hash_table_replace(index_buffer.buffered_fingerprints, &ne->fp, tq);

		index_buffer.num++;
	}

}

extern void index_lookup_similarity_detection(struct segment *s);

/*
 * return 1: indicates lookup is successful.
 * return 0: indicates the index buffer is full.
 */
int index_lookup(struct segment* s) {

	/* Ensure the next phase not be blocked. */
	if (index_buffer.num > 2 * destor.rewrite_algorithm[1]) {
		DEBUG("The index buffer is full (%d chunks in buffer)",
				index_buffer.num);
		return 0;
	}

	if(destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT
			&& destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY
			&& destor.index_segment_selection_method[1] == INDEX_SEGMENT_SELECT_TOP){
		/* Similarity-based */
		s->features = sampling(s->chunks, s->chunk_num);
		index_lookup_similarity_detection(s);
	}else{
		/* Base */
		index_lookup_base(s);
	}

	return 1;
}

/*
 * Input features with an ID.
 */
void index_update(GHashTable *features, int64_t id){
	NOTICE("Filter phase: update %d features", g_hash_table_size(features));
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, features);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		kvstore_update(key, id);
	}
}

/* This function is designed for rewriting. */
void index_check_buffer(struct segment *s) {

	int len = g_queue_get_length(s->chunks), i;

	for (i = 0; i < len; ++i) {
		struct chunk* c = g_queue_peek_nth(s->chunks, i);

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
			continue;

		if((CHECK_CHUNK(c, CHUNK_DUPLICATE) && c->id == TEMPORARY_ID)
				|| CHECK_CHUNK(c, CHUNK_OUT_OF_ORDER)
				|| CHECK_CHUNK(c, CHUNK_SPARSE)){
			/*
			 * 1. A duplicate chunk with a TEMPORARY_ID,
			 * 	  indicates it is a copy of a recent unique chunk.
			 * 2. If we want to rewrite a chunk,
			 *    we check whether it has been rewritten recently.
			 */
			GQueue* bq = g_hash_table_lookup(index_buffer.buffered_fingerprints, &c->fp);
			assert(bq);
			struct indexElem* be = g_queue_peek_head(bq);

			if(be->id > c->id){

				if(c->id != TEMPORARY_ID)
					/* this chunk has been rewritten recently */
					SET_CHUNK(c, CHUNK_REWRITE_DENIED);

				c->id = be->id;
			}
		}
	}
}

/*
 * Return 1 indicates buffer remains full.
 */
int index_update_buffer(struct segment *s){
	int len = g_queue_get_length(s->chunks), i;

	for (i = 0; i < len; ++i) {
		struct chunk* c = g_queue_peek_nth(s->chunks, i);

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
			continue;

		assert(c->id != TEMPORARY_ID);

		GQueue* bq = g_hash_table_lookup(index_buffer.buffered_fingerprints, &c->fp);

		struct indexElem* e = g_queue_pop_head(bq);
		if(g_queue_get_length(bq) == 0){
			g_hash_table_remove(index_buffer.buffered_fingerprints, &c->fp);
			g_queue_free(bq);
		}else{
			int num = g_queue_get_length(bq), j;
			for(j=0; j<num; j++){
				struct indexElem* ie = g_queue_peek_nth(bq, j);
				ie->id = c->id;
			}
		}
		free(e);
		index_buffer.num--;
	}

	if (index_buffer.num <= 2 * destor.rewrite_algorithm[1]) {
		DEBUG("The index buffer is ready for more chunks (%d chunks in buffer)",
				index_buffer.num);
		return 0;
	}
	return 1;
}

