#include "containerstore.h"
#include "../utils/serial.h"
#include "../utils/sync_queue.h"
#include "../jcr.h"

static int64_t container_count = 0;
static FILE* fp;
/* Control the concurrent accesses to fp. */
static pthread_mutex_t mutex;

static pthread_t append_t;

static SyncQueue* container_buffer;


struct metaEntry {
	int32_t off;
	int32_t len;
	/*
	 * the flag indicates whether it is a delta
	 * 0 -> base chunk
	 * 1 -> delta chunk
	 * */
	char flag;
	fingerprint fp;
};

/*
 * We must ensure a container is either in the buffer or written to disks.
 */
static void* append_thread(void *arg) {

	while (1) {
		struct container *c = sync_queue_get_top(container_buffer);
		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		write_container(c);

		TIMER_END(1, jcr.write_time);

		sync_queue_pop(container_buffer);

		free_container(c);
	}

	return NULL;
}

void init_container_store() {

	sds containerfile = sdsdup(destor.working_directory);
	containerfile = sdscat(containerfile, "/container.pool");

	if ((fp = fopen(containerfile, "r+"))) {
		fread(&container_count, 8, 1, fp);
	} else if (!(fp = fopen(containerfile, "w+"))) {
		perror(
				"Can not create containers/container.pool for read and write because");
		exit(1);
	}

	sdsfree(containerfile);

	container_buffer = sync_queue_new(25);

	pthread_mutex_init(&mutex, NULL);

	pthread_create(&append_t, NULL, append_thread, NULL);
}

void close_container_store() {
	sync_queue_term(container_buffer);
	pthread_join(append_t, NULL);

	fseek(fp, 0, SEEK_SET);
	fwrite(&container_count, sizeof(container_count), 1, fp);

	fclose(fp);
	fp = NULL;

	pthread_mutex_destroy(&mutex);
}

static void init_container_meta(struct containerMeta *meta) {
	meta->chunk_num = 0;
	meta->data_size = 0;
	meta->id = TEMPORARY_ID;
	meta->map = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, NULL,
			free);
}

/*
 * For backup.
 */
struct container* create_container() {
	struct container *c = (struct container*) malloc(sizeof(struct container));
	c->data = calloc(1, CONTAINER_SIZE);

	init_container_meta(&c->meta);
	c->meta.id = container_count++;
	return c;
}

containerid get_container_id(struct container* c) {
	return c->meta.id;
}

void write_container_async(struct container* c) {
	assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

	if (container_empty(c)) {
		/* An empty container
		 * It possibly occurs in the end of backup */
		container_count--;
		VERBOSE("Append phase: Deny writing an empty container %lld",
				c->meta.id);
		return;
	}

	sync_queue_push(container_buffer, c);
}

static inline void mark_bitmap(unsigned char* bitmap, int n){
	/* mark the nth bit */
	bitmap[n>>3] |= 1<< (n & 7);
}

/*
 * Called by Append phase
 * HEAD, meta entries, bitmap, data
 */
void write_container(struct container* c) {

	assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

	if (container_empty(c)) {
		/* An empty container
		 * It possibly occurs in the end of backup */
		container_count--;
		VERBOSE("Append phase: Deny writing an empty container %lld",
				c->meta.id);
		return;
	}

	VERBOSE("Append phase: Writing container %lld of %d chunks", c->meta.id,
			c->meta.chunk_num);

	unsigned char *bitmap = malloc((c->meta.chunk_num+7)/8);
	unsigned char *entries = malloc(c->meta.chunk_num * CONTAINER_META_ENTRY);
	int n = 0;

	ser_declare;
	ser_begin(entries, 0);
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, c->meta.map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry *me = (struct metaEntry *) value;
		ser_bytes(&me->fp, sizeof(fingerprint));
		ser_bytes(&me->len, sizeof(int32_t));
		ser_bytes(&me->off, sizeof(int32_t));
		if(me->flag == 1)
			mark_bitmap(bitmap, n);
		n++;
	}

	ser_end(entries, c->meta.chunk_num * CONTAINER_META_ENTRY);

	pthread_mutex_lock(&mutex);

	if (fseek(fp, c->meta.id * CONTAINER_SIZE + 8, SEEK_SET) != 0) {
		perror("Fail seek in container store.");
		exit(1);
	}

	int count = fwrite(&c->meta.id, sizeof(c->meta.id), 1, fp);
	count += fwrite(&c->meta.chunk_num, sizeof(c->meta.chunk_num), 1, fp);
	count += fwrite(&c->meta.data_size, sizeof(c->meta.data_size), 1, fp);

	count += fwrite(bitmap, (c->meta.chunk_num+7)/8, 1, fp);
	count += fwrite(entries, c->meta.chunk_num * CONTAINER_META_ENTRY, 1, fp);

	count += fwrite(c->data, CONTAINER_SIZE - CONTAINER_HEAD - (c->meta.chunk_num+7)/8
			- c->meta.chunk_num * CONTAINER_META_ENTRY, 1, fp);
	assert(count == 6);

	pthread_mutex_unlock(&mutex);

	free(entries);
	free(bitmap);
}

struct container* retrieve_container_by_id(containerid id) {
	struct container *c = (struct container*) malloc(sizeof(struct container));
	init_container_meta(&c->meta);
	c->data = malloc(CONTAINER_SIZE);

	pthread_mutex_lock(&mutex);

	fseek(fp, id * CONTAINER_SIZE + 8, SEEK_SET);

	int count = fread(&c->meta.id, sizeof(c->meta.id), 1, fp);

	if(c->meta.id != id){
		WARNING("expect %lld, but read %lld", id, c->meta.id);
		assert(c->meta.id == id);
	}

	count += fread(&c->meta.chunk_num, sizeof(c->meta.chunk_num), 1, fp);
	count += fread(&c->meta.data_size, sizeof(c->meta.data_size), 1, fp);

	unsigned char *bitmap = malloc((c->meta.chunk_num+7)/8);
	unsigned char *entries = malloc(c->meta.chunk_num * CONTAINER_META_ENTRY);

	count += fread(bitmap, (c->meta.chunk_num+7)/8, 1, fp);
	count += fread(entries, c->meta.chunk_num * CONTAINER_META_ENTRY, 1, fp);

	count += fread(c->data, CONTAINER_SIZE - CONTAINER_HEAD - (c->meta.chunk_num+7)/8
			- c->meta.chunk_num * CONTAINER_META_ENTRY, 1, fp);
	assert(count == 6);

	pthread_mutex_unlock(&mutex);

	unser_declare;
	unser_begin(entries, 0);

	int i;
	for (i = 0; i < c->meta.chunk_num; i++) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		unser_bytes(&me->fp, sizeof(fingerprint));
		unser_bytes(&me->len, sizeof(int32_t));
		unser_bytes(&me->off, sizeof(int32_t));
		me->flag = 0;

		if ((bitmap[i>>3] & (1 << (i & 7))) == 1){
			/* it is a delta */
			me->flag = 1;
		}

		g_hash_table_insert(c->meta.map, &me->fp, me);
	}

	unser_end(entries, c->meta.chunk_num * CONTAINER_META_ENTRY);

	free(entries);
	free(bitmap);
	return c;
}

static struct containerMeta* container_meta_duplicate(struct container *c) {
	struct containerMeta* base = &c->meta;
	struct containerMeta* dup = (struct containerMeta*) malloc(
			sizeof(struct containerMeta));
	init_container_meta(dup);
	dup->id = base->id;
	dup->chunk_num = base->chunk_num;
	dup->data_size = base->data_size;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, base->map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		memcpy(me, value, sizeof(struct metaEntry));
		g_hash_table_insert(dup->map, &me->fp, me);
	}

	return dup;
}

struct containerMeta* retrieve_container_meta_by_id(containerid id) {
	struct containerMeta* cm = NULL;

	/* First, we find it in the buffer */
	cm = sync_queue_find(container_buffer, container_check_id, &id,
			container_meta_duplicate);

	if (cm)
		return cm;

	cm = (struct containerMeta*) malloc(sizeof(struct containerMeta));
	init_container_meta(cm);

	pthread_mutex_lock(&mutex);

	fseek(fp, id * CONTAINER_SIZE + 8, SEEK_SET);

	int count = fread(&cm->id, sizeof(cm->id), 1, fp);

	if(cm->id != id){
		WARNING("expect %lld, but read %lld", id, cm->id);
		assert(cm->id == id);
	}

	count += fread(&cm->chunk_num, sizeof(cm->chunk_num), 1, fp);
	count += fread(&cm->data_size, sizeof(cm->data_size), 1, fp);

	unsigned char *bitmap = malloc((cm->chunk_num+7)/8);
	unsigned char *entries = malloc(cm->chunk_num * CONTAINER_META_ENTRY);

	count += fread(bitmap, (cm->chunk_num+7)/8, 1, fp);
	count += fread(entries, cm->chunk_num * CONTAINER_META_ENTRY, 1, fp);

	assert(count == 5);

	pthread_mutex_unlock(&mutex);

	unser_declare;
	unser_begin(entries, 0);

	int i;
	for (i = 0; i < cm->chunk_num; i++) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		unser_bytes(&me->fp, sizeof(fingerprint));
		unser_bytes(&me->len, sizeof(int32_t));
		unser_bytes(&me->off, sizeof(int32_t));
		me->flag = 0;

		if ((bitmap[i>>3] & (1 << (i & 7))) == 1){
			/* it is a delta */
			me->flag = 1;
		}

		g_hash_table_insert(cm->map, &me->fp, me);
	}

	unser_end(entries, cm->chunk_num * CONTAINER_META_ENTRY);

	free(entries);
	free(bitmap);

	return cm;
}

static struct metaEntry* get_metaentry_in_container_meta(
		struct containerMeta* cm, fingerprint *fp) {
	return g_hash_table_lookup(cm->map, fp);
}

struct chunk* get_chunk_in_container(struct container* c, fingerprint *fp) {
	struct metaEntry* me = get_metaentry_in_container_meta(&c->meta, fp);

	assert(me);

	struct chunk* ck = NULL;
	if(me->flag == 1){
		/* It is a delta */
		int csize = 0;
		int dsize = me->len - sizeof(csize) - sizeof(containerid) - sizeof(fingerprint);

		unser_declare;
		unser_begin(c->data + me->off, me->len);
		unser_int32(csize);
		ck = new_chunk(csize);

		ck->delta = new_delta(dsize);
		unser_bytes(ck->delta->data, dsize);
		unser_int64(ck->delta->baseid);
		unser_bytes(ck->delta->basefp, sizeof(fingerprint));
		unser_end(c->data + me->off, me->len);
		/* The data portion remains unknown */
	}else{
		/* It is not a delta */
		ck = new_chunk(me->len);
		memcpy(ck->data, c->data + me->off, me->len);
	}

	ck->id = c->meta.id;
	memcpy(&ck->fp, &fp, sizeof(fingerprint));

	return ck;
}

static inline int calculate_meta_size(int num){
	int size = CONTAINER_HEAD;
	size += num * CONTAINER_META_ENTRY;
	/* The size of bit map */
	size += (num+7)/8;

	return size;
}

int container_overflow(struct container* c, struct chunk* ck) {

	int capacity = c->meta.data_size;

	if(ck->delta == NULL) {
		capacity += ck->size;
	}else{
		capacity += sizeof(ck->size) + ck->delta->size
				+ sizeof(containerid) + sizeof(fingerprint);
	}

	capacity += calculate_meta_size(c->meta.chunk_num + 1);

	if(capacity > CONTAINER_SIZE)
		return 1;

	return 0;
}

/*
 * For backup.
 * return 1 indicates success.
 * return 0 indicates fail.
 */
int add_chunk_to_container(struct container* c, struct chunk* ck) {
	assert(!container_overflow(c, ck));
	if (g_hash_table_contains(c->meta.map, &ck->fp)) {
		NOTICE("Writing a chunk already in the container buffer!");
		ck->id = c->meta.id;
		return 0;
	}

	struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
	memcpy(&me->fp, &ck->fp, sizeof(fingerprint));
	me->off = c->meta.data_size;

	if(ck->delta != NULL){
		/* it is a delta */
		me->flag = 1;

		ser_declare;
		ser_begin(c->data + c->meta.data_size, 0);
		ser_int32(ck->size);
		ser_bytes(ck->delta->data, ck->delta->size);
		ser_int64(ck->delta->baseid);
		ser_bytes(ck->delta->basefp, sizeof(fingerprint));
		ser_end(c->data + c->meta.data_size, sizeof(ck->size) + ck->delta->size
				+ sizeof(containerid) + sizeof(fingerprint));
		me->len = ser_length(c->data + c->meta.data_size);
		c->meta.data_size += me->len;

	}else{
		/* it is not a delta */
		me->flag = 0;

		memcpy(c->data + c->meta.data_size, ck->data, ck->size);
		c->meta.data_size += ck->size;

		me->len = ck->size;
	}

	g_hash_table_insert(c->meta.map, &me->fp, me);
	c->meta.chunk_num++;

	ck->id = c->meta.id;

	return 1;
}

void free_container_meta(struct containerMeta* cm) {
	g_hash_table_destroy(cm->map);
	free(cm);
}

void free_container(struct container* c) {
	g_hash_table_destroy(c->meta.map);
	if (c->data)
		free(c->data);
	free(c);
}

int container_empty(struct container* c) {
	return c->meta.chunk_num == 0 ? 1 : 0;
}

/*
 * Return 0 if doesn't exist.
 */
int lookup_fingerprint_in_container_meta(struct containerMeta* cm,
		fingerprint *fp) {
	return g_hash_table_lookup(cm->map, fp) == NULL ? 0 : 1;
}

int lookup_fingerprint_in_container(struct container* c, fingerprint *fp) {
	return lookup_fingerprint_in_container_meta(&c->meta, fp);
}

gint g_container_cmp_desc(struct container* c1, struct container* c2,
		gpointer user_data) {
	return g_container_meta_cmp_desc(&c1->meta, &c2->meta, user_data);
}

gint g_container_meta_cmp_desc(struct containerMeta* cm1,
		struct containerMeta* cm2, gpointer user_data) {
	return cm2->id - cm1->id;
}

int container_check_id(struct container* c, containerid* id) {
	return container_meta_check_id(&c->meta, id);
}

int container_meta_check_id(struct containerMeta* cm, containerid* id) {
	return cm->id == *id ? 1 : 0;
}

/*
 * foreach the fingerprints in the container.
 * Apply the 'func' for each fingerprint.
 */
void container_meta_foreach(struct containerMeta* cm, void (*func)(fingerprint*, void*), void* data){
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, cm->map);
	while(g_hash_table_iter_next(&iter, &key, &value)){
		func(key, data);
	}
}
