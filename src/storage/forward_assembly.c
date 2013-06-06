#include "forward_assembly.h"
#include "container.h"
#include "container_volume.h"

extern int simulation_level;
extern double read_container_time;
/* rolling forward assembly area */
static char *assembly_area;
static int64_t area_length;
static int64_t area_offset;

static CFLMonitor *monitor;
static int64_t chunk_num;
static int64_t total_chunk_num;
static JobVolume *job_volume;

static FingerChunk* fchunks_head;
static FingerChunk* fchunks_tail;
static int64_t chunks_length;

static FingerChunk* remaining_fchunk;
/*static BOOL fc_end;*/

static void fill_fchunks() {
	if (chunk_num == total_chunk_num) {
		/*fc_end = TRUE;*/
		return;
	}
	if (remaining_fchunk == 0)
		remaining_fchunk = jvol_read_fingerchunk(job_volume);
	while ((chunks_length + remaining_fchunk->length) < area_length) {
		if (fchunks_head == 0) {
			fchunks_head = remaining_fchunk;
		} else {
			fchunks_tail->next = remaining_fchunk;
		}
		fchunks_tail = remaining_fchunk;

		chunk_num++;
		chunks_length += remaining_fchunk->length;
		if (chunk_num == total_chunk_num) {
			/*fc_end == TRUE;*/
			break;
		}
		remaining_fchunk = jvol_read_fingerchunk(job_volume);
	}
}

static void assemble_area(Container *container) {
	int64_t off = area_offset;
	int32_t len = 0;
	FingerChunk* p = fchunks_head;
	while (p) {
		if (p->container_id == container->id) {
			Chunk *chunk = container_get_chunk(container, &p->fingerprint);
			if (chunk == NULL)
				dprint("container is corrupted!");
			if ((off + chunk->length) > area_length) {
				memcpy(assembly_area + off, chunk->data, area_length - off);
				memcpy(assembly_area, chunk->data + area_length - off,
						off + chunk->length - area_length);
			} else {
				memcpy(assembly_area + off, chunk->data, chunk->length);
			}
			len += chunk->length;
			free_chunk(chunk);
			/* Borrow the TMP_CONTAINER_ID as the filled flag */
			/* mark it */
			p->container_id = TMP_CONTAINER_ID;
		}
		off = (off + p->length) % area_length;
		p = p->next;
	}
	update_cfl_directly(monitor, len, TRUE);
}

void init_assembly_area(int read_cache_size, JobVolume *jvol, int64_t number) {
	monitor = cfl_monitor_new(0);
	area_length = 4L * read_cache_size * 1024 * 1024;
	chunks_length = 0;
	area_offset = 0;
	assembly_area = malloc(area_length);
	fchunks_head = 0;
	fchunks_tail = 0;
	remaining_fchunk = 0;

	job_volume = jvol;
	total_chunk_num = number;
}

CFLMonitor* destroy_assembly_area() {
	free(assembly_area);
	return monitor;
}

Chunk* asm_get_chunk() {
	Chunk *ret_chunk = allocate_chunk();
	fill_fchunks();
	if (fchunks_head->container_id != TMP_CONTAINER_ID) {
		ContainerId cid = fchunks_head->container_id;
		Container *container = NULL;
		TIMER_DECLARE(b, e);
		TIMER_BEGIN(b);
		if (simulation_level >= SIMULATION_RECOVERY) {
			container = read_container_meta_only(cid);
		} else {
			container = read_container(cid);
		}
		TIMER_END(read_container_time, b, e);

		assemble_area(container);
		container_free_full(container);
	}

	ret_chunk->data = malloc(fchunks_head->length);
	ret_chunk->length = fchunks_head->length;
	if ((area_offset + fchunks_head->length) <= area_length) {
		memcpy(ret_chunk->data, assembly_area + area_offset,
				fchunks_head->length);
	} else {
		memcpy(ret_chunk->data, assembly_area + area_offset,
				area_length - area_offset);
		memcpy(ret_chunk->data + area_length - area_offset, assembly_area,
				fchunks_head->length - area_length + area_offset);
	}

	area_offset = (area_offset + fchunks_head->length) % area_length;
	chunks_length -= fchunks_head->length;

	FingerChunk *p = fchunks_head;
	fchunks_head = fchunks_head->next;
	free(p);

	return ret_chunk;
}
