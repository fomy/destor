#ifndef CONTAINER_VOLUME_H_
#define CONTAINER_VOLUME_H_

#include "container.h"

typedef struct container_vol_tag ContainerVolume;

struct container_vol_tag {

    int32_t container_num;
    int file_descriptor;
    char volume_path[40]; //= "/home/fumin/working/container_volume";
    pthread_mutex_t mutex;
};

int init_container_volume();
int update_container_volume();
int destroy_container_volume();
void set_container_id(Container* container);
int32_t seal_container(Container* container);
BOOL append_container(Container* container);
//Chunk* read_chunk_directly(Container *container, Fingerprint *hash);
Container* read_container(ContainerId id);
Container *read_container_meta_only(ContainerId id);

#endif
