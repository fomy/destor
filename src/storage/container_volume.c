/* * container_volume.c *
 *  Created on: Jun 13, 2012
 *      Author: fumin
 */
#include "../global.h"
#include "container.h"
#include "container_volume.h"
#include "../index/index.h"


extern char working_path[];

ContainerVolume container_volume;

/*
 * Init and destroy functions of the globally unique container_volume.
 */
int init_container_volume() {

    strcpy(container_volume.volume_path, working_path);
    strcat(container_volume.volume_path, "container_volume");

    if ((container_volume.file_descriptor = open(container_volume.volume_path,
                    O_RDWR | O_CREAT, S_IRWXU)) <= 0) {
        puts("Failed to init container manager!");
        return FAILURE;
    }

    int32_t count = 0;
    if (read(container_volume.file_descriptor, &count, 4) == 4) {
        container_volume.container_num = count;
    } else {
        container_volume.container_num = 0;
    }

    pthread_mutex_init(&container_volume.mutex, 0);
    return SUCCESS;
}

int update_container_volume(){
    if(pthread_mutex_lock(&container_volume.mutex)){
        printf("%s, %d: failed to lock!\n",__FILE__,__LINE__);
    }
    lseek(container_volume.file_descriptor, 0, SEEK_SET);
    int len = write(container_volume.file_descriptor,
            &container_volume.container_num, 4);
    pthread_mutex_unlock(&container_volume.mutex);
    if (len != 4)
        puts("Failed to write container volume descriptor!");
    return SUCCESS;
}

int destroy_container_volume() {
    update_container_volume();
    pthread_mutex_destroy(&container_volume.mutex);
    close(container_volume.file_descriptor);
    return SUCCESS;
}

void set_container_id(Container *container){
    container->id = container_volume.container_num;
}

/*
 * update fingerprints in this container to index
 * */
int32_t seal_container(Container *container){
    int32_t chunknum = container_get_chunk_num(container);
    if(chunknum == 0)
        return -1;
    container_volume.container_num++;
    return container->id;
}

/*
 * serialize container into bit string.
 * unserialize bit string into hash table.
 */
static char* container_ser(Container* container) {
    if ((g_hash_table_size(container->meta) * CONTAINER_META_ENTRY_SIZE) > CONTAINER_MAX_META_SIZE) {
        dprint("Meta Overflow!");
        return 0;
    }
    char *buff = malloc(CONTAINER_SIZE);

    ser_declare;
    ser_begin(buff, 0);
    ser_int32(container->id);
    ser_int32(container->chunk_num);
    ser_int32(container->data_size);

    GHashTableIter iter;
    g_hash_table_iter_init(&iter, container->meta);
    gpointer key, value;
    while (g_hash_table_iter_next(&iter, &key, &value)) {
        ContainerMetaEntry *cm = (ContainerMetaEntry*) value;
        ser_bytes(&cm->hash, sizeof(Fingerprint));
        ser_int32(cm->offset);
        ser_int32(cm->length);
    }
    ser_bytes(container->data, container->data_size);
    if(ser_length(buff)>CONTAINER_SIZE){
        printf("%s, %d: container %d is overflow.\n",__FILE__,__LINE__,container->id);
    }
    if(ser_length(buff) < CONTAINER_SIZE){
        memset(buff + ser_length(buff), 0, CONTAINER_SIZE-ser_length(buff));
    }
    return buff;
}

static Container* container_unser_meta(char* buff, int32_t size) {
    Container* container = container_new_meta_only();
    unser_declare;
    unser_begin(buff, 0);
    unser_int32(container->id);
    unser_int32(container->chunk_num);
    unser_int32(container->data_size);
    if(container->chunk_num<0){
        dprint("This container is broken!");
        container_free_full(container);
        return 0;
    }
    if((container->chunk_num*28+12)>size){
        printf("id=%d, num: %d, size=%d\n",container->id,container->chunk_num, size);
    }
    int i = 0;
    for(;i<container->chunk_num; ++i){
        ContainerMetaEntry *cm = (ContainerMetaEntry*) malloc(sizeof(ContainerMetaEntry));
        unser_bytes(&cm->hash, sizeof(Fingerprint));
        unser_int32(cm->offset);
        unser_int32(cm->length);
        g_hash_table_replace(container->meta, cm->hash, cm);
    }
    return container;
}

static Container* container_unser_full(char* buff, int32_t size) {
    Container* container = container_new_full();
    unser_declare;
    unser_begin(buff, 0);
    unser_int32(container->id);
    unser_int32(container->chunk_num);
    unser_int32(container->data_size);
    int i = 0;
    for(;i<container->chunk_num;++i){
        ContainerMetaEntry *cm = (ContainerMetaEntry*) malloc(sizeof(ContainerMetaEntry));
        unser_bytes(&cm->hash, sizeof(Fingerprint));
        unser_int32(cm->offset);
        unser_int32(cm->length);
        g_hash_table_replace(container->meta, cm->hash, cm);
    }

    unser_bytes(container->data, container->data_size);
    return container;
}

/*
 * Functions provided for Reading and Writing container volume.
 */
BOOL append_container(Container* container) {
    if (container->chunk_num == 0) {
        puts("append a empty container!");
        return FAILURE;
    }
    check_container(container);

    char *buffer = container_ser(container);
    int len = 0;
    while (TRUE) {
        if(pthread_mutex_lock(&container_volume.mutex)){
            printf("%s, %d:failed to lock!\n",__FILE__,__LINE__);
        }
        if (lseek(container_volume.file_descriptor,
                    ((off_t) container->id) * CONTAINER_SIZE + 4, SEEK_SET) == -1) {
            printf("%s, %d: seal container lseek error!\n", __FILE__, __LINE__);
            free(buffer);
            pthread_mutex_unlock(&container_volume.mutex);
            return FALSE;
        }
        len = write(container_volume.file_descriptor, buffer, CONTAINER_SIZE);
        if (len != CONTAINER_SIZE) {
            puts("Failed to seal container! Retry!");
            pthread_mutex_unlock(&container_volume.mutex);
            continue;
        }
        pthread_mutex_unlock(&container_volume.mutex);
        break;
    }
    free(buffer);
    return TRUE;
}

/*
 * open series functions are used to read a container from volume fully or partially.
 */
Container *read_container_meta_only(ContainerId id) {
    if(id<0){
        printf("%s, %d: container id < 0.\n",__FILE__,__LINE__);
        return NULL;
    }
    char buff[CONTAINER_DES_SIZE+CONTAINER_MAX_META_SIZE];
    if(pthread_mutex_lock(&container_volume.mutex)){
        printf("%s, %d: failed to lock!\n",__FILE__,__LINE__);
    }
    lseek(container_volume.file_descriptor, (off_t) id * CONTAINER_SIZE + 4,
            SEEK_SET);
    int len = read(container_volume.file_descriptor, buff, CONTAINER_DES_SIZE+CONTAINER_MAX_META_SIZE);
    if(len!=(CONTAINER_DES_SIZE+CONTAINER_MAX_META_SIZE)){
        /*pthread_mutex_unlock(&container_volume.mutex);*/
        /*return 0;*/
        dprint("rarely happen!");
    }

    pthread_mutex_unlock(&container_volume.mutex);
    Container *container = container_unser_meta(buff, len);
    return container;
}

Container* read_container(ContainerId id) {
    if(id<0){
        printf("%s, %d: container id < 0.\n",__FILE__,__LINE__);
        return NULL;
    }
    if(pthread_mutex_lock(&container_volume.mutex)){
        printf("%s, %d: failed to lock!\n",__FILE__,__LINE__);
    }
    lseek(container_volume.file_descriptor, (off_t) id * CONTAINER_SIZE + 4,
            SEEK_SET);
    char *buff = malloc(CONTAINER_SIZE);
    int len = read(container_volume.file_descriptor, buff, CONTAINER_SIZE);
    pthread_mutex_unlock(&container_volume.mutex);
    if(len!=CONTAINER_SIZE){
        free(buff);
        return 0;
    }

    Container* container = container_unser_full(buff, len);
    free(buff);
    check_container(container);
    return container;
}

/*
 * enable_data_cache = FALSE
 */
/*Chunk* read_chunk_directly(Container *container, Fingerprint *hash) {*/
    /*Chunk *result = (Chunk*) malloc(sizeof(Chunk));*/
    /*ContainerMetaEntry *cm = container_lookup(container, hash);*/
    /*if (!cm) {*/
        /*puts("Error: Failed to read record directly!");*/
        /*return 0;*/
    /*}*/
    /*off_t offset = 4;*/
    /*offset += container->id * CONTAINER_SIZE;*/
    /*offset += container->chunk_num * CONTAINER_META_ENTRY_SIZE + CONTAINER_DES_SIZE;*/
    /*offset += cm->offset;*/
    /*pthread_mutex_lock(&container_volume.mutex);*/
    /*lseek(container_volume.file_descriptor, offset, SEEK_SET);*/
    /*result->data = malloc(cm->length);*/
    /*result->length = read(container_volume.file_descriptor, result->data,*/
            /*cm->length);*/
    /*pthread_mutex_unlock(&container_volume.mutex);*/
    /*if (result->length != cm->length)*/
        /*puts("Error: Failed to read record data!");*/
    /*memcpy(&result->hash, hash, sizeof(Fingerprint));*/
    /*check_chunk(result);*/
    /*return result;*/
/*}*/
