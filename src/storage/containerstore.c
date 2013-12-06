#include "containerstore.h"
#include "../tools/serial.h"

static int64_t container_count = 0;
static FILE* fp;

struct metaEntry {
	int32_t off;
	int32_t len;
	fingerprint fp;
};

void init_container_store() {

	sds containerfile = sdsdup(destor.working_directory);
	containerfile = sdscat(containerfile, "/containers/container.pool");

	if ((fp = fopen(containerfile, "rw+")) == NULL) {
		fprintf(stderr,
				"Can not open containers/container.pool for read and write\n");
		exit(1);
	}

	int32_t count;
	if (fread(&count, 8, 1, fp) == 1) {
		container_count = count;
	}

	sdsfree(containerfile);
}

void close_container_store() {
	fseek(fp, 0, SEEK_SET);
	fwrite(&container_count, sizeof(container_count), 1, fp);
	fclose(fp);
	fp = NULL;
}

static void init_container_meta(struct containerMeta meta) {
	meta.chunk_num = 0;
	meta.data_size = 0;
	meta.id = TEMPORARY_ID;
	meta.map = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, free,
	NULL);
}

struct container* create_container() {
	struct container *c = (struct container*) malloc(sizeof(struct container));
	init_container_meta(c->meta);
	c->meta.id = container_count++;
	return c;
}

containerid get_container_id(struct container* c) {
	return c->meta.id;
}

void write_container(struct container* c) {

	assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

	unsigned char * cur = &c->data[CONTAINER_SIZE - CONTAINER_META_SIZE];
	ser_declare;
	ser_begin(cur, CONTAINER_META_SIZE);
	ser_int64(c->meta.id);
	ser_int32(c->meta.chunk_num);
	ser_int32(c->meta.data_size);

	GHashTableIter iter;
	gpointer key;
	struct metaEntry *value;
	g_hash_table_iter_init(&iter, c->meta.map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		ser_bytes(&value->fp, sizeof(fingerprint));
		ser_bytes(&value->len, sizeof(int32_t));
		ser_bytes(&value->off, sizeof(int32_t));
	}

	ser_end(cur, CONTAINER_META_SIZE);

	fseek(fp, c->meta.id * CONTAINER_SIZE + 8, SEEK_SET);
	fwrite(c->data, CONTAINER_SIZE, 1, fp);
}

struct container* retrieve_container_by_id(containerid id) {
	struct container *c = (struct container*) malloc(sizeof(struct container));
	init_container_meta(c->meta);

	fseek(fp, id * CONTAINER_SIZE + 8, SEEK_SET);
	fread(c->data, CONTAINER_SIZE, 1, fp);

	unsigned char * cur = &c->data[CONTAINER_SIZE - CONTAINER_META_SIZE];
	unser_declare;
	unser_begin(cur, CONTAINER_META_SIZE);

	unser_int64(c->meta.id);
	unser_int32(c->meta.chunk_num);
	unser_int32(c->meta.data_size);

	assert(c->meta.id == id);

	int i;
	for (i = 0; i < c->meta.chunk_num; i++) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		unser_bytes(&me->fp, sizeof(fingerprint));
		unser_bytes(&me->len, sizeof(int32_t));
		unser_bytes(&me->off, sizeof(int32_t));
		g_hash_table_insert(c->meta.map, &me->fp, &me);
	}

	unser_end(cur, CONTAINER_META_SIZE);

	return c;
}

struct containerMeta* retrieve_container_meta_by_id(containerid id) {
	struct containerMeta* cm = (struct containerMeta*) malloc(
			sizeof(struct containerMeta));
	init_container_meta(*cm);

	unsigned char buf[CONTAINER_META_SIZE];
	fseek(fp, (id + 1) * CONTAINER_SIZE - CONTAINER_META_SIZE + 8, SEEK_SET);
	fread(buf, CONTAINER_META_SIZE, 1, fp);

	unser_declare;
	unser_begin(buf, CONTAINER_META_SIZE);

	unser_int64(cm->id);
	unser_int32(cm->chunk_num);
	unser_int32(cm->data_size);

	assert(cm->id == id);

	int i;
	for (i = 0; i < cm->chunk_num; i++) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		unser_bytes(&me->fp, sizeof(fingerprint));
		unser_bytes(&me->len, sizeof(int32_t));
		unser_bytes(&me->off, sizeof(int32_t));
		g_hash_table_insert(cm->map, &me->fp, &me);
	}

	return cm;
}

static struct metaEntry* get_metaentry_in_container_meta(
		struct containerMeta* cm, fingerprint *fp) {
	return g_hash_table_lookup(cm->map, fp);
}

struct chunk* get_chunk_in_container(struct container* c, fingerprint *fp) {
	struct metaEntry* me = get_metaentry_in_container_meta(&c->meta, fp);

	struct chunk* ck = new_chunk(me->len);

	memcpy(ck->data, c->data + me->off, me->len);
	ck->size = me->len;
	ck->id = c->meta.id;
	memcpy(&ck->fp, &fp, sizeof(fingerprint));

	return ck;
}

int container_overflow(struct container* c, int32_t size) {
	if (c->meta.data_size + size > CONTAINER_SIZE - CONTAINER_META_SIZE)
		return 1;
	/*
	 * 28 is the size of metaEntry.
	 */
	if ((c->meta.chunk_num + 1) * 28 + 16 > CONTAINER_META_SIZE)
		return 1;
	return 0;
}

void add_chunk_to_container(struct container* c, struct chunk* ck) {
	assert(!container_overflow(c, ck->size));
	assert(!g_hash_table_contains(c->meta.map, &ck->fp));

	struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
	memcpy(&me->fp, &ck->fp, sizeof(fingerprint));
	me->len = ck->size;
	me->off = c->meta.data_size;

	g_hash_table_insert(c->meta.map, &me->fp, me);

	memcpy(c->data + c->meta.data_size, ck->data, ck->size);
	c->meta.data_size += ck->size;

	ck->id = c->meta.id;
}

void free_container_meta(struct containerMeta* cm) {
	g_hash_table_destroy(cm->map);
	free(cm);
}

void free_container(struct container* c) {
	g_hash_table_destroy(c->meta.map);
	free(c);
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
