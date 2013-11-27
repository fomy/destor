#include "destor.h"
#include "jcr.h"
#include "storage/container_manage.h"
#include "recipe/recipemanage.h"
#include "tools/sync_queue.h"
#include "rewrite_phase.h"

extern int recv_rewrite_chunk(struct chunk** c);

static struct container *cbuf = NULL;

static pthread_t filter_t;

static SyncQueue* container_queue;

static void send_container(struct container* c) {
	sync_queue_push(container_queue, c);
}

static void term_container_queue() {
	sync_queue_term(container_queue);
}

int recv_container(struct container** c) {
	struct container *rc = sync_queue_pop(container_queue);
	if (rc == NULL)
		return STREAM_END;

	*c = rc;
	return 0;
}

/*
 * Handle containers in container_queue.
 * When a container buffer is full, we push it into container_queue.
 */
static void* filter_thread(void *arg) {
	struct recipe* r = NULL;
	int enable_rewrite = 1;

	while (1) {
		struct chunk* c;
		int size = recv_rewrite_chunk(&c);

		if (size == STREAM_END) {
			/* backup job finish */
			if (r != NULL) {
				append_recipe_meta(jcr.bv, r);
			}
			break;
		} else if (size == FILE_END) {
			if (r != NULL) {
				append_recipe_meta(jcr.bv, r);
				free_recipe(r);
			}
			r = new_recipe(c->data);
			free_chunk(c);
			continue;
		}

		r->filesize += c->size;
		r->chunknum++;

		if (destor.rewrite_enable_cache_aware
				&& restore_aware_contains(c->id)) {
			c->flag |= CHUNK_IN_CACHE;
		}

		if (destor.rewrite_enable_cfl_switch) {
			double cfl = restore_aware_get_cfl();
			if (enable_rewrite && cfl > destor.rewrite_cfl_require)
				enable_rewrite = 0;
			else if (!enable_rewrite && cfl < destor.rewrite_cfl_require)
				enable_rewrite = 1;
		}

		if (CHECK_CHUNK_UNIQUE(c) || CHECK_CHUNK_SPARSE(c)
				|| (enable_rewrite && CHECK_CHUNK_OUT_OF_ORDER(c)
						&& !CHECK_CHUNK_IN_CACHE(c))) {
			/*
			 * If the chunk is unique,
			 * sparse, or out of order and not in cache,
			 * we write it to a container.
			 */
			if (cbuf == NULL)
				cbuf = create_container();

			if (container_overflow(cbuf, c->size)) {

				send_container(cbuf);

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
		append_n_chunk_pointers(jcr.bv, cp, 1);

		/* Collect historical information. */
		har_monitor_update(c->id, c->size);

		/* Restore-aware */
		restore_aware_update(c->id, c->size);

		free_chunk(c);
	}

	if (cbuf) {
		send_container(cbuf);
	}
	term_container_queue();
	return NULL;
}

void start_filter_phase() {
	container_queue = sync_queue_new(25);

	pthread_create(&filter_t, NULL, filter_thread, NULL);
}

void stop_filter_phase() {
	pthread_join(filter_t, NULL);
}
