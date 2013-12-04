/*
 * assembly_restore.c
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */
#include "destor.h"
#include "jcr.h"
#include "recipe/recipestore.h"
#include "storage/containerstore.h"
#include "restore.h"

struct {
	GQueue *area;
	int area_size;
	int size;
} assembly_area;

static void init_assembly_area() {
	assembly_area = g_queue_new();
	assembly_area.size = 0;
	assembly_area.area_size = (destor.restore_cache[1] - 1) * CONTAINER_SIZE;
}

/*
 * Forward assembly.
 * Return a queue of assembled chunks.
 * Return NULL if area is empty.
 */
static GQueue* assemble_area() {
	struct chunk *c = g_queue_peak_head(assembly_area.area);
	if (c == NULL)
		return NULL;

	containerid id = c->id;
	struct container *con;

	if (destor.simulation_level == SIMULATION_NO)
		con = retrieve_container_by_id(id);

	int i, n = g_queue_get_length(assembly_area.area);
	for (i = 0; i < n; i++) {
		struct chunk *c = g_queue_peak_nth(assembly_area.area, i);
		if (id == c->id) {
			if (destor.simulation_level == SIMULATION_NO) {
				struct chunk *buf = get_chunk_in_container(con, &c->fp);
				c->data = malloc(sizeof(c->size));
				memcpy(c->data, buf->data, c->size);
				free_chunk(buf);
			}
			c->flag = CHUNK_READY;
		}
	}

	GQueue *q = g_queue_new();
	while (g_queue_get_length(assembly_area.area) > 0) {
		struct chunk *c = g_queue_peak_head(assembly_area.area);
		if (CHECK_CHUNK_READY(c)) {
			g_queue_pop_head(assembly_area.area);
			g_queue_push_tail(q, c);
		} else {
			break;
		}
	}
	return q;
}

static int assembly_area_push(struct chunkPointer* cp) {
	static struct chunkPointer* last;

	struct chunk *c = new_chunk(0);
	memcpy(&c->fp, &last->fp, sizeof(fingerprint));
	c->id = last->id;
	c->size = last->size;
	c->flag = CHUNK_WAIT;

	g_queue_push_tail(assembly_area.area, c);
	assembly_area.size += c->size;

	/* Indicates end */
	if (cp == NULL)
		return 1;

	last = cp;

	if ((assembly_area.area_size - assembly_area.size) < last->size)
		return 1;

	return 0;
}

void* assembly_restore_thread(void *arg) {
	init_assembly_area();

	GQueue *recipes = g_queue_new();

	struct recipe* r = NULL;
	struct chunkPointer* cp = NULL;

	int sig = recv_restore_recipe(&r);
	int n = 0;

	while (sig != STREAM_END) {

		g_queue_push_tail(recipes, r);

		int num = r->chunknum, i;
		for (i = 0; i < num; i++) {
			sig = recv_restore_recipe(&cp);

			if (assembly_area_push(cp)) {
				/* Full */
				GQueue *q = assemble_area();

				while (g_queue_get_length(q) > 0) {
					if (n == 0) {
						struct recipe *r = g_queue_pop_head(recipes);
						n = r->chunknum;
						send_restore_chunk(r);
					}
					struct chunk* c = g_queue_pop_head(q);
					send_restore_chunk(c);
					n--;
				}

				g_queue_free(q);

			}

		}

		sig = recv_restore_recipe(&r);
	}

	assembly_area_push(NULL);

	GQueue *q;

	while (!(q = assemble_area())) {
		while (g_queue_get_length(q) > 0) {
			if (n == 0) {
				struct recipe *r = g_queue_pop_head(recipes);
				n = r->chunknum;
				send_restore_chunk(r);
			}
			struct chunk* c = g_queue_pop_head(q);
			send_restore_chunk(c);
			n--;
		}

		g_queue_free(q);
	}

	term_restore_chunk_queue();
	return NULL;
}
