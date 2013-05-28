/*
 * restore.c
 *
 *  Created on: Jun 4, 2012
 *      Author: fumin
 */

#include "global.h"
#include "jcr.h"
#include "job/jobmanage.h"
#include "storage/protos.h"
#include "index/index.h"
#include "storage/cfl_monitor.h"
#include "tools/sync_queue.h"

static int restore_one_file(Jcr*, Recipe *psRp);
static void* read_chunk_thread(void *arg);
int restore(Jcr *jcr);

extern int read_cache_size;
extern BOOL enable_data_cache;
extern int read_cache_type;
extern int optimal_cache_window_size;
extern BOOL enable_simulator;

typedef struct{
    char *data;
    int32_t length;
    int32_t offset;
}Record;

static Record* new_record(){
    Record *nrecord = (Record*)malloc(sizeof(Record));
    nrecord->data = 0;
    nrecord->length = 0;
    nrecord->offset = 0;
    return nrecord;
}

static void free_record(Record *record){
    if(record->data){
        free(record->data);
        record->data=0;
    }
    free(record);
}

Record *current_record;

SyncQueue *record_queue;

int restore_server(int revision, char *restore_path) {
    Jcr *jcr = new_read_jcr(read_cache_size, enable_data_cache);

    int jobId = revision;

    if ((jcr->job_volume = open_job_volume(jobId)) == NULL) {
        printf("Doesn't exist such job!\n");
        return FAILURE;
    }

    jcr->job_id = jcr->job_volume->job.job_id;
    jcr->file_num = jcr->job_volume->job.file_num;
    jcr->chunk_num = jcr->job_volume->job.chunk_num;
    strcpy(jcr->backup_path, jcr->job_volume->job.backup_path);

    printf("job id: %d\n", jcr->job_id);
    printf("backup path: %s\n", jcr->job_volume->job.backup_path);

    if (restore_path) {
        strcpy(jcr->restore_path, restore_path);
        strcat(jcr->restore_path, "/");
    } else
        strcpy(jcr->restore_path, jcr->backup_path);


    TIMER_DECLARE(b, e);
    TIMER_BEGIN(b);

    pthread_t read_t;
    current_record = 0;
    record_queue = sync_queue_new(100);
    pthread_create(&read_t, NULL,  read_chunk_thread, jcr);
    if (restore(jcr) != 0) {
        close_job_volume(jcr->job_volume);
        free_jcr(jcr);
        return FAILURE;
    }
    pthread_join(read_t, NULL);

    if(sync_queue_size(record_queue) != 0)
        dprint("record_queue is not empty");
    sync_queue_free(record_queue, free_record);

    TIMER_END(jcr->time, b, e);

    printf("the size of restore job: %ld\n", jcr->job_size);
    printf("elapsed time: %.3fs\n", jcr->time/1000000);
    printf("throughput: %.2fMB/s\n",
            (double) jcr->job_size * 1000000 / (1024 * 1024 * jcr->time));

    printf("read_chunk_time: %.3fs, %.2fMB/s\n",
            jcr->read_chunk_time / 1000000,
            (double) jcr->job_size * 1000000
            / (jcr->read_chunk_time * 1024 * 1024));
    printf("write_file_time: %.3fs, %.2fMB/s\n", jcr->write_file_time / 1000000,
            (double) jcr->job_size * 1000000
            / (jcr->write_file_time * 1024 * 1024));

    close_job_volume(jcr->job_volume);

    free_jcr(jcr);
    return 0;
}

int restore(Jcr *jcr) {

    char *p, *q;
    q = jcr->restore_path + 1;/* ignore the first char*/
    /*
     * recursively make directory
     */
    while ((p = strchr(q, '/'))) {
        if (*p == *(p - 1)) {
            q++;
            continue;
        }
        *p = 0;
        if (access(jcr->restore_path, 0) != 0) {
            mkdir(jcr->restore_path, S_IRWXU | S_IRWXG | S_IRWXO);
        }
        *p = '/';
        q = p + 1;
    }

    if (jcr->file_num >= 1) {
        int number_of_files = jcr->file_num;

        while (number_of_files) {
            Recipe *recipe = read_next_recipe(jcr->job_volume);
            if (restore_one_file(jcr, recipe) != 0)
                printf("failed to restore %s\n", recipe->filename);
            number_of_files--;
            recipe_free(recipe);
        }

    } else {
        puts("No file need to be restored!");
    }
    return SUCCESS;
}

static int restore_one_file(Jcr *jcr, Recipe *recipe) {
    char filepath[512];
    strcpy(filepath, jcr->restore_path);
    strcat(filepath, recipe->filename);
    int len = strlen(jcr->restore_path);
    char *q = filepath+ len;
    char *p;
    while ((p = strchr(q, '/'))) {
        if (*p == *(p - 1)) {
            q++;
            continue;
        }
        *p = 0;
        if (access(filepath, 0) != 0) {
            mkdir(filepath, S_IRWXU | S_IRWXG | S_IRWXO);
        }
        *p = '/';
        q = p + 1;
    }
    puts(filepath);
    int fd = open(filepath, O_CREAT | O_TRUNC | O_WRONLY,
            S_IRWXU | S_IRWXG | S_IRWXO);
    /*
     * retrieve file data
     * jcr->time = 0;
     */
    while (recipe->filesize) {

        if(current_record == 0)
            current_record = sync_queue_pop(record_queue);

        int write_length = current_record->length > recipe->filesize ?
            recipe->filesize:current_record->length;

        TIMER_DECLARE(b1, e1);
        TIMER_BEGIN(b1);
        /* cherish your disk */
        if(enable_simulator == FALSE)
            write(fd, current_record->data + current_record->offset,
                write_length);
        jcr->job_size += write_length;
        TIMER_END(jcr->write_file_time, b1, e1);

        current_record->length -= write_length;
        current_record->offset += write_length;
        recipe->filesize -= write_length;

        if(current_record->length == 0){
            free_record(current_record);
            current_record = 0;
        }
    }

    close(fd);

    return SUCCESS;
}

/* rolling forward assembly area */
static char *assembly_area;
static int64_t area_length;
static int64_t area_offset;

static CFLMonitor *monitor;
static int64_t chunk_num;

static FingerChunk* fchunks_head;
static FingerChunk* fchunks_tail;
static int64_t chunks_length;

static FingerChunk* remaining_fchunk;
/*static BOOL fc_end;*/

void fill_fchunks(Jcr *jcr){
    if(chunk_num == jcr->chunk_num){
        /*fc_end = TRUE;*/
        return;
    }
    if(remaining_fchunk == 0)
        remaining_fchunk = jvol_read_fingerchunk(jcr->job_volume);
    while((chunks_length+remaining_fchunk->length) < area_length){
        if(fchunks_head == 0){
            fchunks_head = remaining_fchunk;
        }else{
            fchunks_tail->next = remaining_fchunk;
        }
        fchunks_tail = remaining_fchunk;

        chunk_num++;
        chunks_length += remaining_fchunk->length;
        if(chunk_num == jcr->chunk_num){
            /*fc_end == TRUE;*/
            break;
        }
        remaining_fchunk = jvol_read_fingerchunk(jcr->job_volume);
    }
}

void assemble_area(Container *container){
    int64_t off = area_offset;
    int32_t len = 0;
    FingerChunk* p = fchunks_head;
    while(p){
        if(p->container_id == container->id){
            Chunk *chunk = container_get_chunk(container, &p->fingerprint);
            if(chunk == NULL)
                dprint("container is corrupted!");
            if((off+chunk->length) > area_length){
                memcpy(assembly_area + off, chunk->data, area_length - off);
                memcpy(assembly_area, chunk->data + area_length - off
                        , off + chunk->length - area_length);
            }else{
                memcpy(assembly_area + off, chunk->data, chunk->length);
            }
            len += chunk->length;
            free_chunk(chunk);
            p->container_id = TMP_CONTAINER_ID;
        }
        off = (off+p->length)%area_length;
        /* mark it */
        p = p->next;
    }
    update_cfl_directly(monitor, len, TRUE);
}

void send_record(){
    FingerChunk* p = fchunks_head;
    int len = 0;
    while(p && p->container_id == TMP_CONTAINER_ID){
        len += p->length;
        p=p->next;
        free(fchunks_head);
        fchunks_head = p;
    }
    Record *record = new_record();
    record->data = malloc(len);
    if((area_offset + len) <= area_length){
        memcpy(record->data, assembly_area + area_offset, len);
    }else{
        memcpy(record->data, assembly_area + area_offset, area_length - area_offset);
        memcpy(record->data + area_length - area_offset,
                assembly_area, len - area_length + area_offset);
    }
    record->length = len;
    area_offset = (area_offset + len)%area_length;
    chunks_length -= len;
    sync_queue_push(record_queue, record);
}

static void* read_chunk_thread(void *arg) {
    Jcr *jcr = (Jcr*)arg;
    monitor = 0;
    if(read_cache_type == LRU_CACHE){
        puts("cache=LRU");
        jcr->read_cache = container_cache_new(jcr->read_cache_size, jcr->enable_data_cache, jcr->job_id);
    }else if(read_cache_type == OPT_CACHE){
        puts("cache=OPT");
        jcr->read_opt_cache = optimal_container_cache_new(jcr->read_cache_size,
                jcr->enable_data_cache, jcr->job_volume->job_seed_file, optimal_cache_window_size);
    }else if(read_cache_type == ASM_CACHE){
        puts("cache=ASM");
        monitor = cfl_monitor_new(0);
        jcr->asm_buffer = 0;
        area_length = 4L*jcr->read_cache_size*1024*1024;
        chunks_length = 0;
        area_offset = 0;
        assembly_area = malloc(area_length);
        fchunks_head = 0;
        fchunks_tail = 0;
        remaining_fchunk = 0;
        /*fc_end = FALSE;*/
    }
    chunk_num = 0;
    printf("cache_size=%d\n", read_cache_size);

    if(read_cache_type != ASM_CACHE){
        while(chunk_num < jcr->chunk_num){
            FingerChunk* fchunk = jvol_read_fingerchunk(jcr->job_volume);
            Chunk *result = 0;

            TIMER_DECLARE(b1, e1);
            TIMER_BEGIN(b1);
            if(read_cache_type == LRU_CACHE){
                result = container_cache_get_chunk(jcr->read_cache,
                        &fchunk->fingerprint, fchunk->container_id);
            }else if(read_cache_type == OPT_CACHE){
                result = optimal_container_cache_get_chunk(jcr->read_opt_cache,
                        &fchunk->fingerprint, fchunk->container_id);
            }
            TIMER_END(jcr->read_chunk_time, b1, e1);
            free(fchunk);
            Record* record = new_record();
            record->data = malloc(result->length);
            memcpy(record->data, result->data, result->length);
            record->length = result->length;
            record->offset = 0;
            free_chunk(result);
            sync_queue_push(record_queue, record);

            chunk_num++;
        }
    }else{
        fill_fchunks(jcr);
        while(chunks_length > 0){
            TIMER_DECLARE(b1, e1);
            TIMER_BEGIN(b1);
            ContainerId cid = fchunks_head->container_id;
            Container *container = read_container(cid);
            assemble_area(container);
            container_free_full(container);
            TIMER_END(jcr->read_chunk_time, b1, e1);

            send_record();

            fill_fchunks(jcr);
        }
    }

    if(read_cache_type == LRU_CACHE)
        monitor = container_cache_free(jcr->read_cache);
    else if(read_cache_type == OPT_CACHE)
        monitor = optimal_container_cache_free(jcr->read_opt_cache);
    else if(read_cache_type == ASM_CACHE){
        free(assembly_area);
    }

    char logfile[40];
    strcpy(logfile, "restore.log");
    int fd = open(logfile, O_WRONLY | O_CREAT, S_IRWXU);
    lseek(fd, 0, SEEK_END);
    char buf[100];
    /*
     * job id,
     * minimal container number,
     * actually read container number,
     * CFL,
     * speed factor
     * throughput
     */
    sprintf(buf, "%d %d %d %.2f %.4f %.2f\n", jcr->job_id, 
            monitor->ocf, monitor->ccf,  get_cfl(monitor),
            1.0*jcr->job_size/(monitor->ccf*1024.0*1024),
            (double) jcr->job_size * 1000000 / (jcr->read_chunk_time * 1024 * 1024));
    write(fd, buf, strlen(buf));
    close(fd);

    print_cfl(monitor);
    cfl_monitor_free(monitor);

    return NULL;
}
