/*
 * backup.c
 *
 * Our statistics only measure the efficiency of index and rewriting algorithms.
 * Even some duplicates may be found while adding to container,
 * we ignore it.
 * So the container volume may seems inconsistency with statistics.
 *
 *  Created on: Jun 4, 2012
 *      Author: fumin
 */
#include "global.h"
#include "jcr.h"
#include "job/jobmanage.h"
#include "storage/protos.h"
#include "statistic.h"
#include "tools/sync_queue.h"
#include "index/index.h"

extern DestorStat *destor_stat;
extern int rewriting_algorithm;
extern int fingerprint_index_type;

void* read_thread(void *argv);
int backup(Jcr* jcr);

SyncQueue* read_queue;

/* chunk queue */
SyncQueue* chunk_queue;

/* hash queue */
SyncQueue* hash_queue;

/* prepare queue */
SyncQueue* prepare_queue;

/* container queue */
SyncQueue *container_queue;

int backup_server(char *path) {

    Jcr *jcr = new_write_jcr();
    strcpy(jcr->backup_path, path);
    if (access(jcr->backup_path, 4) != 0) {
        free(jcr);
        puts("This path does not exist or can not be read!");
        return -1;
    }

    if(index_init() == FALSE){
        return -1;
    }

    puts("==== backup begin ====");
    puts("==== transfering begin ====");
    jcr->job_id = get_next_job_id();
    if (is_job_existed(jcr->job_id)) {
        printf("job existed!\n");
        free(jcr);
        return FAILURE;
    }
    jcr->job_volume = create_job_volume(jcr->job_id);

    struct timeval begin, end;
    gettimeofday(&begin, 0);
    if (backup(jcr) != 0) {
        free(jcr);
        return FAILURE;
    }
    gettimeofday(&end, 0);

    index_destroy();

    jcr->time = end.tv_sec - begin.tv_sec
        + (double) (end.tv_usec - begin.tv_usec) / (1024 * 1024);
    puts("==== transferring end ====");

    printf("job id: %d\n", jcr->job_id);
    printf("backup path: %s\n", jcr->backup_path);
    printf("number of files: %d\n", jcr->file_num);
    printf("number of chunks: %d\n", jcr->chunk_num);
    printf("number of dup chunks: %d\n", jcr->number_of_dup_chunks);
    printf("total size: %ld\n", jcr->job_size);
    printf("dedup size: %ld\n", jcr->dedup_size);
    printf("dedup efficiency: %.2f\n",
            jcr->job_size!= 0 ?
            (double) (jcr->dedup_size) / (double) (jcr->job_size) :
            0);
    printf("elapsed time: %.3fs\n", jcr->time);
    printf("throughput: %.2fMB/s\n",
            (double) jcr->job_size/ (1024 * 1024 * jcr->time));
    printf("zero chunk count: %d\n", jcr->zero_chunk_count);
    printf("zero_chunk_amount: %ld\n", jcr->zero_chunk_amount);
    printf("rewritten_chunk_count: %d\n", jcr->rewritten_chunk_count);
    printf("rewritten_chunk_amount: %ld\n", jcr->rewritten_chunk_amount);
    printf("rewritten rate in amount: %.3f\n", jcr->rewritten_chunk_amount/(double)jcr->job_size);
    printf("rewritten rate in count: %.3f\n", jcr->rewritten_chunk_count/(double)jcr->chunk_num);

    destor_stat->data_amount += jcr->job_size;
    destor_stat->consumed_capacity += jcr->job_size - jcr->dedup_size;
    destor_stat->saved_capacity += jcr->dedup_size;
    destor_stat->number_of_chunks += jcr->chunk_num;
    destor_stat->number_of_dup_chunks += jcr->number_of_dup_chunks;
    destor_stat->zero_chunk_count += jcr->zero_chunk_count;
    destor_stat->zero_chunk_amount += jcr->zero_chunk_amount;
    destor_stat->rewritten_chunk_count += jcr->rewritten_chunk_count;
    destor_stat->rewritten_chunk_amount += jcr->rewritten_chunk_amount;

    printf("read_time : %.3fs, %.2fMB/s\n", jcr->read_time / 1000000,
            jcr->job_size * 1000000 / jcr->read_time / 1024 / 1024);
    printf("chunk_time : %.3fs, %.2fMB/s\n", jcr->chunk_time / 1000000,
            jcr->job_size * 1000000 / jcr->chunk_time / 1024 / 1024);
    printf("name_time : %.3fs, %.2fMB/s\n", jcr->name_time / 1000000,
            jcr->job_size * 1000000 / jcr->name_time / 1024 / 1024);
    printf("filter_time : %.3fs, %.2fMB/s\n", jcr->filter_time / 1000000,
            jcr->job_size * 1000000 / jcr->filter_time / 1024 / 1024);
    printf("write_time : %.3fs, %.2fMB/s\n", jcr->write_time / 1000000,
            jcr->job_size * 1000000 / jcr->write_time / 1024 / 1024);
    puts("==== backup end ====");

    jcr->job_volume->job.job_id= jcr->job_id;
    jcr->job_volume->job.is_del = FALSE;
    jcr->job_volume->job.file_num= jcr->file_num;
    jcr->job_volume->job.chunk_num = jcr->chunk_num;

    strcpy(jcr->job_volume->job.backup_path, jcr->backup_path);

    update_job_volume_des(jcr->job_volume);
    close_job_volume(jcr->job_volume);
    jcr->job_volume= 0;

    char logfile[] = "backup.log";
    int fd = open(logfile, O_WRONLY | O_CREAT, S_IRWXU);
    lseek(fd, 0, SEEK_END);
    char buf[100];
    /* 
     * job id, 
     * consumed capacity,
     * dedup ratio,
     * rewritten ratio,
     * total container number,
     * sparse container number,
     * inherited container number,
     * throughput
     */
    sprintf(buf, "%d %ld %.2f %.2f %d %d %d %.2f\n", jcr->job_id, destor_stat->consumed_capacity,
            jcr->job_size != 0 ?(double) (jcr->dedup_size) / (double) (jcr->job_size):0,
            jcr->job_size != 0 ?(double) (jcr->rewritten_chunk_amount) / (double) (jcr->job_size):0,
            jcr->total_container_num,
            jcr->sparse_container_num,
            jcr->inherited_sparse_num,
            (double) jcr->job_size / (1024 * 1024 * jcr->time));
    if(write(fd, buf, strlen(buf))!=strlen(buf)){
    }
    close(fd);
    free_jcr(jcr);
    return SUCCESS;

}

int backup(Jcr* jcr) {
    pthread_t read_t, chunk_t, hash_t, prepare_t, filter_t, append_t;
    read_queue = sync_queue_new(2);
    chunkAlg_init();
    chunk_queue = sync_queue_new(100);
    hash_queue = sync_queue_new(100);
    prepare_queue = sync_queue_new(100);
    container_queue = sync_queue_new(100);

    pthread_create(&read_t, NULL, read_thread, jcr);
    pthread_create(&chunk_t, NULL, rabin_chunk, jcr);
    pthread_create(&hash_t, NULL, sha1_hash, jcr);
    switch(fingerprint_index_type){
        case RAM_INDEX:
        case DDFS_INDEX:
            pthread_create(&prepare_t, NULL, simply_prepare, jcr);
            break;
        case EXBIN_INDEX:
            pthread_create(&prepare_t, NULL, exbin_prepare, jcr);
            break;
        case SILO_INDEX:
            pthread_create(&prepare_t, NULL, silo_prepare, jcr);
            break;
        default:
            dprint("wrong index type!");
    }
    if (rewriting_algorithm == NO_REWRITING) {
        puts("rewriting_algorithm=NO");
        pthread_create(&filter_t, NULL, simply_filter, jcr);
    } else if(rewriting_algorithm == CFL_REWRITING){
        puts("rewriting_algorithm=CFL");
        pthread_create(&filter_t, NULL, cfl_filter, jcr);
    } else if(rewriting_algorithm == CBR_REWRITING){
        puts("rewriting_algorithm=CBR");
        pthread_create(&filter_t, NULL, cbr_filter, jcr);
    } else if(rewriting_algorithm == HBR_REWRITING){
        puts("rewriting_algorithm=HBR");
        pthread_create(&filter_t, NULL, simply_filter, jcr);
    } else if(rewriting_algorithm == HBR_CBR_REWRITING){
        puts("rewriting_algorithm=HBR_CBR");
        pthread_create(&filter_t, NULL, cbr_filter, jcr);
    } else if(rewriting_algorithm == CAP_REWRITING){
        puts("rewriting_algorithm=CAP");
        pthread_create(&filter_t, NULL, cap_filter, jcr);
    } else if(rewriting_algorithm == HBR_CAP_REWRITING){
        puts("rewriting_algorithm=HBR_CAP");
        pthread_create(&filter_t, NULL, cap_filter, jcr);
    } else{
        dprint("invalid rewriting algorithm\n");
        exit(-1);
    }
    pthread_create(&append_t, NULL, append_thread, jcr);

    ContainerId seed_id = -1;
    int32_t seed_len = 0;
    FingerChunk* fchunk = (FingerChunk*)sync_queue_pop(jcr->fingerchunk_queue);
    while(fchunk->container_id!=STREAM_END){
        jvol_append_fingerchunk(jcr->job_volume, fchunk);

        if(seed_id!=-1 && seed_id!=fchunk->container_id){
            jvol_append_seed(jcr->job_volume, seed_id, seed_len);
            seed_len = 0;
        }
        /* merge sequential accesses */
        seed_id = fchunk->container_id;
        seed_len += fchunk->length;

        free(fchunk);
        fchunk = (FingerChunk*)sync_queue_pop(jcr->fingerchunk_queue);
    }
    free(fchunk);
    if(seed_len > 0)
        jvol_append_seed(jcr->job_volume, seed_id, seed_len);

    /* store recipes of processed file */
    int i = 0;
    for(;i < jcr->file_num; ++i) {
        Recipe *recipe = (Recipe*) sync_queue_pop(jcr->completed_files_queue);
        recipe->fileindex = i;
        if (jvol_append_meta(jcr->job_volume, recipe) != SUCCESS) {
            printf("%s, %d: some errors happened in appending recipe!\n",
                    __FILE__, __LINE__);
            return FAILURE;
        }
        jcr->chunk_num += recipe->chunknum;
        recipe_free(recipe);
    }

    pthread_join(read_t, NULL);
    pthread_join(chunk_t, NULL);
    pthread_join(hash_t, NULL);
    pthread_join(prepare_t, NULL);
    pthread_join(filter_t, NULL);
    pthread_join(append_t, NULL);

    sync_queue_free(chunk_queue, 0);
    sync_queue_free(hash_queue, 0);
    sync_queue_free(prepare_queue, 0);
    sync_queue_free(container_queue, 0);
    sync_queue_free(read_queue, 0);

    return 0;
}

void* read_thread(void *argv) {
    /* Each file will be processed seperately */
    Jcr *jcr = (Jcr*) argv;

    struct stat state;
    if (stat(jcr->backup_path, &state) != 0) {
        puts("backup path does not exist!");
        return NULL;
    }
    if (S_ISREG(state.st_mode)) { //single file
        char filepath[512];
        strcpy(filepath, jcr->backup_path);

        char *p = jcr->backup_path + strlen(jcr->backup_path) - 1;
        while (*p != '/')
            --p;
        *(p + 1) = 0;

        read_file(jcr, filepath);
    } else {
        if (find_one_file(jcr, jcr->backup_path) != 0) {
            puts("something wrong!");
            return NULL;
        }
    }

    DataBuffer *signal = (DataBuffer*)malloc(sizeof(DataBuffer));
    signal->size = STREAM_END;
    sync_queue_push(read_queue, signal);
    return NULL;
}

int find_one_file(Jcr *jcr, char *path) {
    struct stat state;
    if (stat(path, &state) != 0) {
        puts("file does not exist! ignored!");
        return 0;
    }
    if (S_ISDIR(state.st_mode)) {
        /*puts("This is a directory!");*/
        DIR *dir = opendir(path);
        struct dirent *entry;
        char newpath[512];
        if (strcmp(path + strlen(path) - 1, "/")) {
            strcat(path, "/");
        }

        while ((entry = readdir(dir)) != 0) {
            /*ignore . and ..*/
            if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
                continue;
            strcpy(newpath, path);
            strcat(newpath, entry->d_name);
            if (find_one_file(jcr, newpath) != 0) {
                return -1;
            }
        }
        //printf("*** out %s direcotry ***\n", path);
        closedir(dir);
    } else if (S_ISREG(state.st_mode)) {
        /*printf("*** %s\n", path);*/

        read_file(jcr, path);

    } else {
        puts("illegal file type! ignored!");
        return 0;
    }
    return 0;
}

int read_file(Jcr *jcr, char *path) {

    char filename[256];
    int len = strlen(jcr->backup_path);
    strcpy(filename, path + len);

    Recipe *recipe = recipe_new();
    jcr->file_num++;
    if ((recipe->fd = open(path, O_RDONLY)) <= 0) {
        printf("%s, %d: Can not open file!\n", __FILE__, __LINE__);
        return -1;
    } 
    strcpy(recipe->filename, filename);
    sync_queue_push(jcr->waiting_files_queue, recipe);

    TIMER_DECLARE(b, e);
    TIMER_BEGIN(b);
    int length = 0;
    DataBuffer *new_data_buffer = (DataBuffer*) malloc(sizeof(DataBuffer));
    while ((length = read(recipe->fd, new_data_buffer->data, READ_BUFFER_SIZE))) {
        TIMER_END(jcr->read_time, b, e);

        new_data_buffer->size = length;
        jcr->job_size += new_data_buffer->size;
        sync_queue_push(read_queue, new_data_buffer);
        new_data_buffer = (DataBuffer*) malloc(sizeof(DataBuffer));
        TIMER_BEGIN(b);
    }
    new_data_buffer->size = FILE_END;
    sync_queue_push(read_queue, new_data_buffer);
    return 0;
}
