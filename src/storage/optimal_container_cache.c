/**
 * @file optimal_container_cache.c
 * @Synopsis  optimal cache. 
 * @author fumin, fumin@hust.edu.cn
 * @version 1
 * @date 2013-01-03
 */
#include "optimal_container_cache.h"
#include "container_volume.h"
#include "../dedup.h"

#define INFINITE -1
extern int simulation_level;
extern double read_container_time;
static int read_nseed(FILE *seed_file, Seed *start, int count) {
	if (count == 0)
		return;
	int i = 0;
	for (; i < count; i++) {
		if (fscanf(seed_file, "%d:%d\n", &start[i].container_id,
				&start[i].remaining_size) != 2) {
			break;
		}
	}
	return i;
}

static DistanceItem* distance_item_new() {
	DistanceItem* new_item = (DistanceItem*) malloc(sizeof(DistanceItem));
	new_item->distance_queue = queue_new();
	return new_item;
}

static void distance_item_free(DistanceItem *item) {
	queue_free(item->distance_queue, free);
	free(item);
}

/* NOTE: descending ordering */
static gint distance_cmp_func(gconstpointer a, gconstpointer b,
		gpointer user_data) {
	if (((DistanceItem*) b)->next_position == INFINITE) {
		return 1;
	}
	if (((DistanceItem*) a)->next_position == INFINITE) {
		return -1;
	}
	return ((DistanceItem*) b)->next_position
			- ((DistanceItem*) a)->next_position;
}

static void distance_table_add_item(GHashTable *distance_table,
		ContainerId container_id, int32_t distance) {
	DistanceItem* item = g_hash_table_lookup(distance_table, &container_id);
	if (item) {
		if (item->next_position == INFINITE) {
			item->next_position = distance;
			item->furthest_position = distance;
		} else {
			int32_t *new_distance = (int32_t*) malloc(sizeof(int32_t));
			*new_distance = distance;
			queue_push(item->distance_queue, new_distance);
			item->furthest_position = distance;
		}
	} else {
		item = distance_item_new();
		item->container_id = container_id;
		item->next_position = distance;
		item->furthest_position = distance;
		g_hash_table_insert(distance_table, &item->container_id, item);
	}
}

static GHashTable* distance_table_init(Seed* window, int window_size) {
	GHashTable* distance_table = g_hash_table_new_full(g_int_hash, g_int_equal,
			NULL, distance_item_free);

	int i;
	for (i = 0; i < window_size; ++i) {
		distance_table_add_item(distance_table, window[i].container_id, i + 1);
	}
	return distance_table;
}

static ContainerPool* container_pool_new() {
	ContainerPool* container_pool = (ContainerPool*) malloc(
			sizeof(ContainerPool));
	container_pool->pool = g_hash_table_new_full(g_int_hash, g_int_equal, NULL,
			NULL);
	container_pool->distance_sequence = g_sequence_new(NULL);
	return container_pool;
}

static void container_pool_free(ContainerPool* container_pool) {
	g_sequence_free(container_pool->distance_sequence);
	g_hash_table_destroy(container_pool->pool);
	free(container_pool);
}

/*static void distance_decrease(gpointer key, gpointer value, gpointer user_data){*/
/*int32_t window_size = *(int32_t*)user_data;*/
/*DistanceItem* item = (DistanceItem*)value;*/
/*if(item->next_position > window_size)*/
/*return;*/
/*item->next_position--;*/
/*item->furthest_position--;*/
/*if(item->next_position<0)*/
/*printf("%s, %d: error happens\n",__FILE__,__LINE__);*/
/*if(item->next_position == 0){*/
/* This operation may change the sequence in container pool,
 * so we sort container pool after this. */
/*int32_t *next_position= queue_pop(item->distance_queue);*/
/*if(next_position){*/
/*item->next_position = *next_position;*/
/*free(next_position);*/
/*}else{*/
/*if(item->furthest_position != 0)*/
/*printf("%s, %d: It can not happen!\n",__FILE__,__LINE__);*/
/*item->next_position= *(int32_t*)user_data+1;*/
/*item->furthest_position = item->next_position;*/
/*}   */
/*}*/
/*}*/

static Seed* seed_window_slide(OptimalContainerCache *opt_cache) {
	if (opt_cache->seed_count == 0) {
		printf("%s, %d: no other seeds\n", __FILE__, __LINE__);
		return NULL;
	}
	if (opt_cache->seed_file_is_end == FALSE
			&& opt_cache->seed_count == opt_cache->window_size) {
		/* read more seed */
		memmove(opt_cache->seed_buffer,
				&opt_cache->seed_buffer[opt_cache->window_start],
				sizeof(Seed) * opt_cache->window_size);
		opt_cache->window_start = 0;
		int32_t new_seed_count = read_nseed(opt_cache->seed_file,
				&opt_cache->seed_buffer[opt_cache->window_size],
				opt_cache->window_size);
		if (new_seed_count < opt_cache->window_size) {
			opt_cache->seed_file_is_end = TRUE;
		}
		opt_cache->seed_count += new_seed_count;
	}
	/* -1 */
	Seed *new_active_seed = &opt_cache->seed_buffer[opt_cache->window_start];
	/*printf("%d, %d\n", new_active_seed->container_id, new_active_seed->remaining_size);*/
	DistanceItem* distance = g_hash_table_lookup(opt_cache->distance_table,
			&new_active_seed->container_id);
	if (distance->next_position == opt_cache->current_distance) {
		/*This operation may change the sequence in container pool,*/
		/*so we sort container pool after this. */
		int32_t *next_position = queue_pop(distance->distance_queue);
		if (next_position) {
			distance->next_position = *next_position;
			free(next_position);
		} else {
			if (distance->furthest_position != opt_cache->current_distance)
				printf("%s, %d: It can not happen!\n", __FILE__, __LINE__);
			distance->next_position = INFINITE;
			distance->furthest_position = INFINITE;
		}
	} else {
		dprint("error happen!");
	}

	opt_cache->window_start++;
	opt_cache->window_size--;
	opt_cache->seed_count--;

	/* +1 */
	if (opt_cache->seed_count > opt_cache->window_size) {
		opt_cache->window_size++;
		Seed *seed = &opt_cache->seed_buffer[opt_cache->window_start
				+ opt_cache->window_size - 1];
		distance_table_add_item(opt_cache->distance_table, seed->container_id,
				opt_cache->current_distance + opt_cache->window_size);
	}

	/* neccesary */
	GSequenceIter* last_item = g_sequence_iter_prev(
			g_sequence_get_end_iter(
					opt_cache->container_pool->distance_sequence));
	if (!g_sequence_iter_is_end(last_item))
		g_sequence_sort_changed(last_item, distance_cmp_func, NULL);
	return new_active_seed;
}

static Container* container_pool_insert(ContainerPool *container_pool,
		int cache_size, Container* container, DistanceItem* distance) {
	Container* evictor = 0;
	if (g_hash_table_size(container_pool->pool) >= cache_size) {
		/* evict */
		GSequenceIter* begin = g_sequence_get_begin_iter(
				container_pool->distance_sequence);
		DistanceItem* evictor_distance = g_sequence_get(begin);
		g_sequence_remove(begin);
		evictor = g_hash_table_lookup(container_pool->pool,
				&evictor_distance->container_id);
		g_hash_table_remove(container_pool->pool, &evictor->id);
	}
	g_hash_table_insert(container_pool->pool, &container->id, container);

	/* descending order */
	g_sequence_insert_sorted(container_pool->distance_sequence, distance,
			distance_cmp_func, NULL);
	return evictor;
}

/* 
 * INTERFACES: 
 * optimal_container_cache_new, initialize cache;
 * optimal_container_cache_free, destroy cache;
 * optimal_container_get_chunk
 * */
OptimalContainerCache* optimal_container_cache_new(int cache_size,
		BOOL enable_data, FILE *seed_file, int window_size) {
	OptimalContainerCache *opt_cache = (OptimalContainerCache*) malloc(
			sizeof(OptimalContainerCache));
	opt_cache->cache_size = cache_size;
	opt_cache->enable_data = enable_data;
	if (enable_data)
		opt_cache->cfl_monitor = cfl_monitor_new(cache_size);
	else
		opt_cache->cfl_monitor = 0;

	/* read seeds twice the size of seed_window. */
	opt_cache->seed_file = seed_file;
	opt_cache->seed_buffer = (Seed*) malloc(2 * sizeof(Seed) * window_size);
	opt_cache->seed_file_is_end = FALSE;
	if ((opt_cache->seed_count = read_nseed(opt_cache->seed_file,
			opt_cache->seed_buffer, 2 * window_size)) == 0) {
		free(opt_cache->seed_buffer);
		fclose(opt_cache->seed_file);
		free(opt_cache);
		return NULL;
	}
	/*opt_cache->total_seed_count = opt_cache->seed_count;*/
	opt_cache->current_distance = 0;
	/* initialize seed_window */
	opt_cache->window_size =
			window_size < opt_cache->seed_count ?
					window_size : opt_cache->seed_count;
	opt_cache->window_start = 0;

	/* init distance_table */
	opt_cache->distance_table = distance_table_init(opt_cache->seed_buffer,
			opt_cache->window_size);

	/*opt_cache->active_container = 0;*/
	opt_cache->map = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
			free, g_sequence_free);
	opt_cache->active_seed = 0;

	/* id-container pair */
	opt_cache->container_pool = container_pool_new();

	return opt_cache;
}

CFLMonitor* optimal_container_cache_free(OptimalContainerCache *opt_cache) {
	CFLMonitor *monitor = opt_cache->cfl_monitor;
	g_hash_table_destroy(opt_cache->map);
	container_pool_free(opt_cache->container_pool);

	g_hash_table_destroy(opt_cache->distance_table);

	free(opt_cache->seed_buffer);

	free(opt_cache);
	return monitor;
}

static Container *optimal_container_cache_lookup(
		OptimalContainerCache *opt_cache, Fingerprint *finger) {
	Container *container = 0;
	GSequence *container_list = g_hash_table_lookup(opt_cache->map, finger);
	if (container_list) {
		container = g_sequence_get(g_sequence_get_begin_iter(container_list));
	}
	return container;

}

static Container *optimal_container_cache_insert(
		OptimalContainerCache *opt_cache, ContainerId container_id) {
	/* read and insert container */
	Container *required_container = 0;
	TIMER_DECLARE(b, e);
	TIMER_BEGIN(b);
	if (opt_cache->enable_data) {
		if (simulation_level >= SIMULATION_RECOVERY) {
			required_container = read_container_meta_only(container_id);
		} else {
			required_container = read_container(container_id);
		}
	} else {
		required_container = read_container_meta_only(container_id);
	}
	TIMER_END(read_container_time, b, e);

	DistanceItem *distance = g_hash_table_lookup(opt_cache->distance_table,
			&container_id);
	Container *evictor = container_pool_insert(opt_cache->container_pool,
			opt_cache->cache_size, required_container, distance);

	/* 40% */
	if (evictor) {
		int32_t chunknum = container_get_chunk_num(evictor);
		Fingerprint* fingers = container_get_all_fingers(evictor);
		int i = 0;
		for (; i < chunknum; ++i) {
			GSequence* container_list = g_hash_table_lookup(opt_cache->map,
					&fingers[i]);
			g_sequence_remove(
					g_sequence_lookup(container_list, evictor,
							container_cmp_des, NULL));
			if (g_sequence_get_length(container_list) == 0) {
				g_hash_table_remove(opt_cache->map, &fingers[i]);
			}
		}
		free(fingers);

		DistanceItem *evict_distance = g_hash_table_lookup(
				opt_cache->distance_table, &evictor->id);
		if ((evict_distance->next_position == INFINITE)) {
			g_hash_table_remove(opt_cache->distance_table,
					&evict_distance->container_id);
		}
		container_free_full(evictor);
	}

	int32_t num = container_get_chunk_num(required_container);
	Fingerprint *nfingers = container_get_all_fingers(required_container);
	int i = 0;
	/* 50% */
	for (; i < num; ++i) {
		GSequence* container_list = g_hash_table_lookup(opt_cache->map,
				&nfingers[i]);
		if (container_list == NULL) {
			container_list = g_sequence_new(NULL);
			Fingerprint *finger = (Fingerprint*) malloc(sizeof(Fingerprint));
			memcpy(finger, &nfingers[i], sizeof(Fingerprint));
			g_hash_table_insert(opt_cache->map, finger, container_list);
		}
		g_sequence_insert_sorted(container_list, required_container,
				container_cmp_des, NULL);
	}
	free(nfingers);

	return required_container;
}

Chunk* optimal_container_cache_get_chunk(OptimalContainerCache* opt_cache,
		Fingerprint *finger, ContainerId container_id) {
	/* slide window */
	if (opt_cache->active_seed == NULL
			|| opt_cache->active_seed->remaining_size == 0) {
		/* next container */
		opt_cache->current_distance++;
		opt_cache->active_seed = seed_window_slide(opt_cache);
	}

	if (opt_cache->active_seed->container_id != container_id) {
		printf("%s, %d: inconsistent id\n", __FILE__, __LINE__);
		return NULL;
	}

	BOOL is_new = FALSE;
	Chunk *result = 0;
	Container *container = optimal_container_cache_lookup(opt_cache, finger);
	if (container) {
		result = container_get_chunk(container, finger);
		is_new = FALSE;
	} else {
		container = optimal_container_cache_insert(opt_cache, container_id);
		result = container_get_chunk(container, finger);
		is_new = TRUE;
	}

	if (!result) {
		printf("%s, %d: container does not contain this fingerprint\n",
				__FILE__, __LINE__);
		return NULL;
	}
	update_cfl_directly(opt_cache->cfl_monitor, result->length, is_new);
	opt_cache->active_seed->remaining_size -= result->length;
	return result;
}
