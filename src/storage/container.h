#ifndef CONTAINER_H_
#define CONTAINER_H_

#include "../dedup.h"

#define CONTAINER_DES_SIZE 12
#define CONTAINER_META_ENTRY_SIZE 28
#define CONTAINER_SIZE (4*1024*1024LL) //4MB
#define CONTAINER_MAX_META_SIZE (64*1024)

#define CONTAINER_FULL -1
#define WRITE_SUCCESS 0

typedef struct container_tag Container;

typedef struct {
	char hash[20];
	int32_t offset;
	int32_t length;
} ContainerMetaEntry;

struct container_tag {
	/* Container descriptor */
	ContainerId id;
	int32_t data_size;
	int32_t chunk_num;
	int32_t used_size;

	GHashTable *meta;
	char *data;
};

GList* container_get_chunk_list(Container* container);
Container* container_new_meta_only(); //read cache meta only
Container* container_new_full(); //write buffer, read cache
void container_free_full(Container* container); //read cache
Fingerprint* container_get_all_fingers(Container* container); //
double container_get_usage(Container* container);
gint container_cmp_asc(gconstpointer a, gconstpointer b);
gint container_cmp_des(gconstpointer a, gconstpointer b, gpointer user_data);
int32_t container_add_chunk(Container* container, Chunk* chunk); //
Chunk* container_get_chunk(Container* container, Fingerprint *hash);
BOOL container_contains(Container* container, Fingerprint* hash); //write buffer
int32_t container_get_chunk_num(Container*);
BOOL check_container(Container*);
BOOL container_equal(Container* c1, Container* c2);

#endif
