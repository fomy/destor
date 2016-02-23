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
	GSequence *area;
	int64_t area_size;
	int64_t size;
} assembly_area;

static void init_assembly_area() {
	assembly_area.area = g_sequence_new(NULL);
	assembly_area.size = 0;
	assembly_area.area_size = (destor.restore_cache[1] - 1) * CONTAINER_SIZE;
}

/*
 * Forward assembly.
 * Return a queue of assembled chunks.
 * Return NULL if area is empty.
 */
static GQueue* assemble_area() {

	if (g_sequence_get_length(assembly_area.area) == 0)
		return NULL;

	GQueue *q = g_queue_new();

	struct chunk *c = NULL;
	GSequenceIter *begin = g_sequence_get_begin_iter(assembly_area.area);
	GSequenceIter *end = g_sequence_get_end_iter(assembly_area.area);
	for (;begin != end;begin = g_sequence_get_begin_iter(assembly_area.area)) {
		c = g_sequence_get(begin);
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			g_sequence_remove(begin);
			g_queue_push_tail(q, c);
			c = NULL;
		} else {
			break;
		}
	}

	/* !c == true indicates no more chunks in the area. */
	if (!c)
		return q;

	/* read a container */
	containerid id = c->id;
	struct container *con = NULL;
	jcr.read_container_num++;
	VERBOSE("Restore cache: container %lld is missed", id);
	if (destor.simulation_level == SIMULATION_NO)
		con = retrieve_container_by_id(id);

	/* assemble the area */
	GSequenceIter *iter = g_sequence_get_begin_iter(assembly_area.area);
	end = g_sequence_get_end_iter(assembly_area.area);
	for (;iter != end;iter = g_sequence_iter_next(iter)) {
		c = g_sequence_get(iter);
		if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
				&& id == c->id) {
			if (destor.simulation_level == SIMULATION_NO) {
				struct chunk *buf = get_chunk_in_container(con, &c->fp);
				assert(c->size == buf->size);
				c->data = malloc(c->size);
				memcpy(c->data, buf->data, c->size);
				free_chunk(buf);
			}
			SET_CHUNK(c, CHUNK_READY);
		}
	}

	/* issue the assembled area */
	begin = g_sequence_get_begin_iter(assembly_area.area);
	end = g_sequence_get_end_iter(assembly_area.area);
	for (;begin != end;begin = g_sequence_get_begin_iter(assembly_area.area)) {
		struct chunk *rc = g_sequence_get(begin);
		if (CHECK_CHUNK(rc, CHUNK_FILE_START) || CHECK_CHUNK(rc, CHUNK_FILE_END)){
			g_sequence_remove(begin);
			g_queue_push_tail(q, rc);
		}else if(CHECK_CHUNK(rc, CHUNK_READY)) {
			g_sequence_remove(begin);
			g_queue_push_tail(q, rc);
			assembly_area.size -= rc->size;
		} else {
			break;
		}
	}
	return q;
}

static int assembly_area_push(struct chunk* c) {
	/* Indicates end */
	if (c == NULL)
		return 1;

	g_sequence_append(assembly_area.area, c);

	if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
		return 0;

	assembly_area.size += c->size;

	if (assembly_area.size >= assembly_area.area_size)
		return 1;

	return 0;
}

void* assembly_restore_thread(void *arg) {
	init_assembly_area();

	struct chunk* c;
	while ((c = sync_queue_pop(restore_recipe_queue))) {

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (assembly_area_push(c)) {
			/* Full */
			GQueue *q = assemble_area();

			TIMER_END(1, jcr.read_chunk_time);

			struct chunk* rc;
			while ((rc = g_queue_pop_head(q))) {
				if (CHECK_CHUNK(rc, CHUNK_FILE_START) 
						|| CHECK_CHUNK(rc, CHUNK_FILE_END)) {
					sync_queue_push(restore_chunk_queue, rc);
					continue;
				}
				jcr.data_size += rc->size;
				jcr.chunk_num++;
				if (destor.simulation_level >= SIMULATION_RESTORE) {
					/* Simulating restore. */
					free_chunk(rc);
				} else {
					sync_queue_push(restore_chunk_queue, rc);
				}
			}

			g_queue_free(q);
		} else {
			TIMER_END(1, jcr.read_chunk_time);
		}

	}

	assembly_area_push(NULL);

	GQueue *q;
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	while ((q = assemble_area())) {
		TIMER_END(1, jcr.read_chunk_time);
		struct chunk* rc;
		while ((rc = g_queue_pop_head(q))) {

			if (CHECK_CHUNK(rc,CHUNK_FILE_START) 
					|| CHECK_CHUNK(rc, CHUNK_FILE_END)) {
				sync_queue_push(restore_chunk_queue, rc);
				continue;
			}

			jcr.data_size += rc->size;
			jcr.chunk_num++;
			if (destor.simulation_level >= SIMULATION_RESTORE) {
				/* Simulating restore. */
				free_chunk(rc);
			} else {
				sync_queue_push(restore_chunk_queue, rc);
			}
		}
		TIMER_BEGIN(1);
		g_queue_free(q);
	}

	sync_queue_term(restore_chunk_queue);
	return NULL;
}
