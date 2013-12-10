#include "destor.h"
#include "jcr.h"
#include "storage/containerstore.h"
#include "recipe/recipestore.h"
#include "rewrite_phase.h"
#include "backup.h"

static pthread_t filter_t;

/*
 * Handle containers in container_queue.
 * When a container buffer is full, we push it into container_queue.
 */
static void* filter_thread(void *arg) {
	/* container buffer */
	struct container *cbuf = NULL;
	int enable_rewrite = 1;

	while (1) {
		struct chunk* c = sync_queue_pop(rewrite_queue);

		if (c == NULL)
			/* backup job finish */
			break;

		assert(CHECK_CHUNK(c, CHUNK_FILE_START));
		struct recipe* r = new_recipe(c->data);
		free_chunk(c);

		c = sync_queue_pop(rewrite_queue);

		while (!(CHECK_CHUNK(c, CHUNK_FILE_END))) {
			r->filesize += c->size;
			r->chunknum++;

			if (destor.rewrite_enable_cache_aware
					&& restore_aware_contains(c->id))
				SET_CHUNK(c, CHUNK_IN_CACHE);

			if (destor.rewrite_enable_cfl_switch) {
				double cfl = restore_aware_get_cfl();
				if (enable_rewrite && cfl > destor.rewrite_cfl_require)
					enable_rewrite = 0;
				else if (!enable_rewrite && cfl < destor.rewrite_cfl_require)
					enable_rewrite = 1;
			}

			if (!CHECK_CHUNK(c, CHUNK_DUPLICATE) || CHECK_CHUNK(c, CHUNK_SPARSE)
					|| (enable_rewrite && CHECK_CHUNK(c, CHUNK_OUT_OF_ORDER)
							&& !CHECK_CHUNK(c, CHUNK_IN_CACHE))) {
				/*
				 * If the chunk is unique,
				 * sparse, or out of order and not in cache,
				 * we write it to a container.
				 */
				if (cbuf == NULL)
					cbuf = create_container();

				if (container_overflow(cbuf, c->size)) {

					sync_queue_push(container_queue, cbuf);

					cbuf = create_container();
				}
				containerid ret = index_update(c->fp, c->id,
						get_container_id(cbuf));
				if (ret == TEMPORARY_ID)
					add_chunk_to_container(cbuf, c);
				else
					c->id = ret;
			} else {
				index_update(c->fp, c->id, c->id);
			}
			struct chunkPointer* cp = (struct chunkPointer*) malloc(
					sizeof(struct chunkPointer));
			cp->id = c->id;
			memcpy(&cp->fp, &c->fp, sizeof(fingerprint));
			cp->size = c->size;
			append_n_chunk_pointers(jcr.bv, cp, 1);

			/* Collect historical information. */
			har_monitor_update(c->id, c->size);

			/* Restore-aware */
			restore_aware_update(c->id, c->size);

			free_chunk(c);

			c = sync_queue_pop(rewrite_queue);
		}

		free_chunk(c);
		append_recipe_meta(jcr.bv, r);
		free_recipe(r);
	}

	if (cbuf)
		sync_queue_push(container_queue, cbuf);
	sync_queue_term(container_queue);
	return NULL;
}

void start_filter_phase() {
	container_queue = sync_queue_new(25);

	pthread_create(&filter_t, NULL, filter_thread, NULL);
}

void stop_filter_phase() {
	pthread_join(filter_t, NULL);
}
