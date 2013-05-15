#include "global.h"
#include "tools/sync_queue.h"
#include "jcr.h"

static SyncQueue* read_queue;
static pthread_t read_t;

static void send_data(DataBuffer* data_buffer){
    sync_queue_push(read_queue, data_buffer);
}

static void signal_data(int signal){
    DataBuffer *buffer= (DataBuffer*)malloc(sizeof(DataBuffer));
    buffer->size = signal;
    send_data(buffer);
}

int recv_data(DataBuffer** data_buffer){
    DataBuffer* buffer = sync_queue_pop(read_queue);
    if(buffer->size == FILE_END){
        free(buffer);
        *data_buffer = NULL;
        return FILE_END;
    }else if(buffer->size == STREAM_END){
        free(buffer);
        *data_buffer = NULL;
        return STREAM_END;
    }
    *data_buffer = buffer;
    return SUCCESS;
}

static int read_file(Jcr *jcr, char *path) {

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
        send_data(new_data_buffer);
        new_data_buffer = (DataBuffer*) malloc(sizeof(DataBuffer));
        TIMER_BEGIN(b);
    }
    free(new_data_buffer);
    signal_data(FILE_END);
    return 0;
}

static int find_one_file(Jcr *jcr, char *path) {
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

static void* read_thread(void *argv) {
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

    signal_data(STREAM_END);
    return NULL;
}

int start_read_phase(Jcr *jcr){
    read_queue = sync_queue_new(2);
    pthread_create(&read_t, NULL, read_thread, jcr);
}

void stop_read_phase(){
    pthread_join(read_t, NULL);
}


