#include "destor.h"
#include "jcr.h"
#include "storage/containerstore.h"
#include "recipe/recipestore.h"
#include "rewrite_phase.h"
#include "backup.h"
#include "index/index.h"
#include "index/segmentstore.h"

static pthread_t filter_t;
static int64_t chunk_num;

struct{
	/* accessed in dedup phase */
	struct container *container_buffer;
	/* In order to facilitate sampling in container,
	 * we keep a queue for chunks in container buffer. */
	GQueue *chunks;
} storage_buffer;


extern struct {
	/* g_mutex_init() is unnecessary if in static storage. */
	GMutex mutex;
	GCond not_full_cond; // index buffer is not full
	int wait_flag;
} index_lock;

/*
 * When a container buffer is full, we push it into container_queue.
 */
static void* filter_thread(void *arg) {
    int enable_rewrite = 1;
    struct recipe* r = NULL;


    while (1) {
        struct chunk* c = sync_queue_pop(rewrite_queue);

        if (c == NULL)
            /* backup job finish */
            break;

        /* reconstruct a segment */
        struct segment* s = new_segment();

        /* segment head */
        assert(CHECK_CHUNK(c, CHUNK_SEGMENT_START));
        free_chunk(c);

        c = sync_queue_pop(rewrite_queue);
        while (!(CHECK_CHUNK(c, CHUNK_SEGMENT_END))) {
            g_queue_push_tail(s->chunks, c);
            if (!CHECK_CHUNK(c, CHUNK_FILE_START)
                    && !CHECK_CHUNK(c, CHUNK_FILE_END)){
            	/* History-Aware Rewriting */
                if (destor.rewrite_enable_har && CHECK_CHUNK(c, CHUNK_DUPLICATE))
                    har_check(c);
                s->chunk_num++;
            }
            c = sync_queue_pop(rewrite_queue);
        }
        free_chunk(c);

        g_mutex_lock(&index_lock.mutex);

        /* For self-references in a segment.
         * If we find there is an early copy of the chunk in this segment,
         * has been rewritten,
         * the rewrite request for it will be denied. */
        GHashTable *recently_rewritten_chunks = g_hash_table_new_full(g_int64_hash,
        		g_fingerprint_equal, NULL, free_chunk);
        GHashTable *recently_unique_chunks = g_hash_table_new_full(g_int64_hash,
        			g_fingerprint_equal, NULL, free_chunk);

        /* This function will check the fragmented chunks
         * that would be rewritten later.
         * If we find an early copy of the chunk in earlier segments,
         * has been rewritten,
         * the rewrite request for it will be denied. */
        index_check_buffer(s);

        int len = g_queue_get_length(s->chunks), i;
        for(i = 0; i<len; i++){
            struct chunk* c = g_queue_peek_nth(s->chunks, i);

    		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
    			continue;

            VERBOSE("Filter phase: %dth chunk in %s container %lld", chunk_num,
                    CHECK_CHUNK(c, CHUNK_OUT_OF_ORDER) ? "out-of-order" : "", c->id);

            /* Cache-Aware Filter */
            if (destor.rewrite_enable_cache_aware && restore_aware_contains(c->id)) {
                assert(c->id != TEMPORARY_ID);
                VERBOSE("Filter phase: %dth chunk is cached", chunk_num);
                SET_CHUNK(c, CHUNK_IN_CACHE);
            }

            /* A cfl-switch for rewriting out-of-order chunks. */
            if (destor.rewrite_enable_cfl_switch) {
                double cfl = restore_aware_get_cfl();
                if (enable_rewrite && cfl > destor.rewrite_cfl_require) {
                    VERBOSE("Filter phase: Turn OFF the (out-of-order) rewrite switch of %.3f",
                            cfl);
                    enable_rewrite = 0;
                } else if (!enable_rewrite && cfl < destor.rewrite_cfl_require) {
                    VERBOSE("Filter phase: Turn ON the (out-of-order) rewrite switch of %.3f",
                            cfl);
                    enable_rewrite = 1;
                }
            }

            if(CHECK_CHUNK(c, CHUNK_DUPLICATE) && c->id == TEMPORARY_ID){
            	struct chunk* ruc = g_hash_table_lookup(recently_unique_chunks, &c->fp);
            	assert(ruc);
            	c->id = ruc->id;
            }
            struct chunk* rwc = g_hash_table_lookup(recently_rewritten_chunks, &c->fp);
            if(rwc){
            	c->id = rwc->id;
            	SET_CHUNK(c, CHUNK_REWRITE_DENIED);
            }

            /* A fragmented chunk will be denied if it has been rewritten recently */
            if (!CHECK_CHUNK(c, CHUNK_DUPLICATE) || (!CHECK_CHUNK(c, CHUNK_REWRITE_DENIED)
            		&& (CHECK_CHUNK(c, CHUNK_SPARSE)
                    || (enable_rewrite && CHECK_CHUNK(c, CHUNK_OUT_OF_ORDER)
                        && !CHECK_CHUNK(c, CHUNK_IN_CACHE))))) {
                /*
                 * If the chunk is unique, or be fragmented and not denied,
                 * we write it to a container.
                 * Fragmented indicates: sparse, or out of order and not in cache,
                 */
                if (storage_buffer.container_buffer == NULL){
                	storage_buffer.container_buffer = create_container();
                	if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY)
                		storage_buffer.chunks = g_queue_new();
                }

                if (container_overflow(storage_buffer.container_buffer, c->size)) {

                    if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY){
                        /*
                         * TO-DO
                         * Update_index for physical locality
                         */
                        GHashTable *features = sampling(storage_buffer.chunks,
                        		g_queue_get_length(storage_buffer.chunks));
                        index_update(features, get_container_id(storage_buffer.container_buffer));
                        g_hash_table_destroy(features);
                        g_queue_free_full(storage_buffer.chunks, free_chunk);
                        storage_buffer.chunks = g_queue_new();
                    }
                    write_container_async(storage_buffer.container_buffer);
                    storage_buffer.container_buffer = create_container();
                }

                if(add_chunk_to_container(storage_buffer.container_buffer, c)){

                	struct chunk* wc = new_chunk(0);
                	memcpy(&wc->fp, &c->fp, sizeof(fingerprint));
                	wc->id = c->id;
                	if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
                		jcr.unique_chunk_num++;
                		jcr.unique_data_size += c->size;
                		g_hash_table_insert(recently_unique_chunks, &wc->fp, wc);
                    	VERBOSE("Filter phase: %dth chunk is recently unique, size %d", chunk_num,
                    			g_hash_table_size(recently_unique_chunks));
                	} else {
                		jcr.rewritten_chunk_num++;
                		jcr.rewritten_chunk_size += c->size;
                		g_hash_table_insert(recently_rewritten_chunks, &wc->fp, wc);
                	}

                	if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY){
                		struct chunk* ck = new_chunk(0);
                		memcpy(&ck->fp, &c->fp, sizeof(fingerprint));
                		g_queue_push_tail(storage_buffer.chunks, ck);
                	}

                	VERBOSE("Filter phase: Write %dth chunk to container %lld",
                			chunk_num, c->id);
                }else{
                	VERBOSE("Filter phase: Deny writing %dth chunk to container %lld",
                			chunk_num, c->id);
                	struct chunk* wc = new_chunk(0);
                	memcpy(&wc->fp, &c->fp, sizeof(fingerprint));
                	wc->id = c->id;
                	/* a fake unique chunk */
                	g_hash_table_insert(recently_unique_chunks, &wc->fp, wc);
                }
            }else{
                if (CHECK_CHUNK(c, CHUNK_OUT_OF_ORDER)) {
                    VERBOSE("Filter phase: %lldth chunk in out-of-order container %lld is already cached",
                            chunk_num, c->id);
                }else if(CHECK_CHUNK(c, CHUNK_REWRITE_DENIED)){
                    VERBOSE("Filter phase: %lldth fragmented chunk is denied", chunk_num);
                }
            }

            assert(c->id != TEMPORARY_ID);

            /* Collect historical information. */
            har_monitor_update(c->id, c->size);

            /* Restore-aware */
            restore_aware_update(c->id, c->size);

            chunk_num++;
        }

        int full = index_update_buffer(s);

        if(destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY){
            /*
             * TO-DO
             * Update_index for logical locality
             */
        	struct segmentRecipe* sr = new_segment_recipe_full(s);

        	sr = update_segment(sr);
            s->features = sampling(s->chunks, s->chunk_num);
        	if(destor.index_category[0] == INDEX_CATEGORY_EXACT){
        		/*
        		 * For exact deduplication,
        		 * unique fingerprints are inserted.
        		 */
        		VERBOSE("Filter phase: add %d unique fingerprints to %d features",
        				g_hash_table_size(recently_unique_chunks),
        				g_hash_table_size(s->features));
        		GHashTableIter iter;
        		gpointer key, value;
        		g_hash_table_iter_init(&iter, recently_unique_chunks);
        		while(g_hash_table_iter_next(&iter, &key, &value)){
        			struct chunk* uc = value;
        			fingerprint *ft = malloc(sizeof(fingerprint));
        			memcpy(ft, &uc->fp, sizeof(fingerprint));
        			g_hash_table_insert(s->features, ft, NULL);
        		}
        	}
        	index_update(s->features, sr->id);
        	free_segment_recipe(sr);
        }

        if(index_lock.wait_flag == 1 && full == 0){
        	index_lock.wait_flag = 0;
        	g_cond_broadcast(&index_lock.not_full_cond);
        }
        g_hash_table_destroy(recently_rewritten_chunks);
        g_hash_table_destroy(recently_unique_chunks);
        g_mutex_unlock(&index_lock.mutex);

        /* Write recipe */
       	while((c = g_queue_pop_head(s->chunks))){

        	if(r == NULL){
        		assert(CHECK_CHUNK(c,CHUNK_FILE_START));
        		r = new_recipe(c->data);
        		free_chunk(c);
        	}else if(!CHECK_CHUNK(c,CHUNK_FILE_END)){
        		struct chunkPointer* cp = (struct chunkPointer*)malloc(sizeof(struct chunkPointer));
        		cp->id = c->id;
        		memcpy(&cp->fp, &c->fp, sizeof(fingerprint));
        		cp->size = c->size;
        		append_n_chunk_pointers(jcr.bv, cp ,1);
        		free(cp);
        		free_chunk(c);
        		r->chunknum++;
        		r->filesize += c->size;
        	}else{
        		append_recipe_meta(jcr.bv, r);
        		free_recipe(r);
        		r = NULL;
        		free_chunk(c);
        	}
        }

        free_segment(s, free_chunk);

    }

    if (storage_buffer.container_buffer
    		&& !container_empty(storage_buffer.container_buffer)){
        if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY){
            /*
             * TO-DO
             * Update_index for physical locality
             */
        	GHashTable *features = sampling(storage_buffer.chunks,
        			g_queue_get_length(storage_buffer.chunks));
        	index_update(features, get_container_id(storage_buffer.container_buffer));
        	g_hash_table_destroy(features);
        	g_queue_free_full(storage_buffer.chunks, free_chunk);
        }
        write_container_async(storage_buffer.container_buffer);
    }

    return NULL;
}

void start_filter_phase() {

	storage_buffer.container_buffer = NULL;

    init_har();

    init_restore_aware();

    pthread_create(&filter_t, NULL, filter_thread, NULL);
}

void stop_filter_phase() {
    pthread_join(filter_t, NULL);
    close_har();
}
