/**
 * @file ddfsindex.c
 * @Synopsis  Data Domain File System, in FAST'08.
 *  We use mysql for simple.
 * @author fumin, fumin@hust.edu.cn
 * @version 1
 * @date 2013-01-09
 */
#include "../global.h"
#include "../storage/container_cache.h"
#include "../tools/lru_cache.h"
#include "../tools/bloom_filter.h"
#include <mysql/mysql.h>

extern char working_path[];
extern int ddfs_cache_size;

static char indexpath[256];
static MYSQL *mdb = 0;
static ContainerCache *fingers_cache;
static unsigned char* filter;

static char search_sql[] = "select ContainerId from HashStore where Fingerprint=? limit 1;";
static char insert_sql[] = "insert into HashStore(Fingerprint, ContainerId) values(?, ?) on duplicate key update ContainerId=?;";

static MYSQL_STMT *search_stmt;
static MYSQL_STMT *insert_stmt;

static BOOL dirty = FALSE;

static BOOL db_open_database(){
    mdb = mysql_init(NULL);
    if(!mysql_real_connect(mdb, "localhost", "destor",
                "123456", "destor_db", 0, NULL, CLIENT_FOUND_ROWS)){
        printf("%s, %d: failed to open database!\n",__FILE__,__LINE__);
        return FALSE;
    }
    return TRUE;
}

static ContainerId db_lookup_fingerprint(Fingerprint *finger){
    unsigned long hashlen = sizeof(Fingerprint);
    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));//Without this, excute and fetch will cause segmentation fault.
    param[0].buffer_type = MYSQL_TYPE_BLOB;
    param[0].buffer = finger;
    param[0].buffer_length = hashlen;
    param[0].length = &hashlen;

    if(mysql_stmt_bind_param(search_stmt, param)!=0){
        printf("%s, %d: %s\n",__FILE__,__LINE__, mysql_stmt_error(search_stmt));
        return -1;
    }
    if(mysql_stmt_execute(search_stmt)!=0){
        printf("%s, %d: failed to search index! %s\n",__FILE__,__LINE__, mysql_stmt_error(search_stmt));
        return -1;
    }

    ContainerId resultId = -1;
    unsigned long reslen = 0;
    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));
    my_bool is_null;
    result[0].buffer_type = MYSQL_TYPE_LONG;
    result[0].buffer = &resultId;
    result[0].length = &reslen;
    result[0].is_null = &is_null;

    if(mysql_stmt_bind_result(search_stmt, result)){
        printf("%s, %d: %s\n",__FILE__,__LINE__, mysql_stmt_error(search_stmt));
        return -1;
    }
    if(mysql_stmt_store_result(search_stmt)){
        printf("%s, %d: %s\n",__FILE__,__LINE__, mysql_stmt_error(search_stmt));
        return -1;
    }
    int ret = mysql_stmt_fetch(search_stmt);
    if(ret == MYSQL_NO_DATA){
        /*printf("%s, %d: no such line.\n",__FILE__,__LINE__);*/
        return -1;
    }else if(ret == 1){
        printf("%s, %d: %s\n",__FILE__,__LINE__, mysql_stmt_error(search_stmt));
        return -1;
    }
    mysql_stmt_free_result(search_stmt);
    return resultId;
}

/* interfaces */
BOOL ddfs_index_init(){
    if(!db_open_database()){
        return FALSE;
    }
    search_stmt = mysql_stmt_init(mdb);
    insert_stmt = mysql_stmt_init(mdb);
    mysql_stmt_prepare(search_stmt, search_sql, strlen(search_sql));
    mysql_stmt_prepare(insert_stmt, insert_sql, strlen(insert_sql));

    fingers_cache = container_cache_new(ddfs_cache_size, FALSE);

    /* read bloom filter */
    strcpy(indexpath, working_path);
    strcat(indexpath, "index/bloom_filter");
    int fd;
    if ((fd = open(indexpath, O_RDONLY | O_CREAT, S_IRWXU)) <= 0) {
        printf("Can not open index/bloom_filter!");
        return FALSE;
    }
    filter = malloc(FILTER_SIZE_BYTES);
    if(FILTER_SIZE_BYTES!=read(fd, filter, FILTER_SIZE_BYTES)){
        bzero(filter, FILTER_SIZE_BYTES);
    }
    close(fd);

    dirty = FALSE;
    return TRUE;
}

void ddfs_index_flush(){
    if(dirty == FALSE)
        return;
    /* flush bloom filter */
    int fd;
    if ((fd = open(indexpath, O_WRONLY | O_CREAT, S_IRWXU)) <= 0) {
        printf("Can not open index/bloom_filter!");
    }
    if(FILTER_SIZE_BYTES!=write(fd, filter, FILTER_SIZE_BYTES)){
        printf("%s, %d: Failed to flush bloom filter!\n",__FILE__,__LINE__);
    }
    close(fd);

    container_cache_free(fingers_cache);
    fingers_cache = container_cache_new(ddfs_cache_size, FALSE);

    dirty = FALSE;
}

void ddfs_index_destroy(){
    mysql_close(mdb);
    mysql_stmt_close(search_stmt);
    mysql_stmt_close(insert_stmt);

    ddfs_index_flush();
    free(filter);
}

ContainerId ddfs_index_search(Fingerprint *finger){
    ContainerId resultId = TMP_CONTAINER_ID;
    /* search in cache */
    Container *container = container_cache_lookup(fingers_cache, finger);
    if(container != 0){
        return container->id;
    }

    /* search in bloom filter */
    if(!in_dict(filter, (char*)finger, sizeof(Fingerprint))){
        return TMP_CONTAINER_ID;
    }

    /* search in database */
    resultId = db_lookup_fingerprint(finger);

    if(resultId != TMP_CONTAINER_ID){
        container_cache_insert_container(fingers_cache, resultId);
    }

    return resultId;
}

/*void ddfs_index_update(Fingerprint* fingers, int32_t fingernum, ContainerId id){*/
    /*unsigned long hashlen = sizeof(Fingerprint);*/
    /*int i = 0;*/
    /*for(;i<fingernum; ++i){*/
        /*MYSQL_BIND param[3];*/
        /*memset(param, 0, sizeof(param));*/
        /*param[0].buffer_type = MYSQL_TYPE_BLOB;*/
        /*param[0].buffer = &fingers[i];*/
        /*param[0].buffer_length = hashlen;*/
        /*param[0].length = &hashlen;*/
        /*param[1].buffer_type = MYSQL_TYPE_LONG;*/
        /*param[1].buffer = &id;*/
        /*param[2].buffer_type = MYSQL_TYPE_LONG;*/
        /*param[2].buffer = &id;*/

        /*if(mysql_stmt_bind_param(insert_stmt, param)){*/
            /*printf("%s, %d: failed to update index! %s\n",__FILE__,__LINE__, mysql_stmt_error(insert_stmt));*/
        /*}*/
        /*if(mysql_stmt_execute(insert_stmt)!=0){*/
            /*printf("%s, %d: failed to update index! %s\n",__FILE__,__LINE__, mysql_stmt_error(insert_stmt));*/
        /*}*/

        /*insert_word(filter, (char*)&fingers[i], sizeof(Fingerprint));*/
    /*}*/
    /*dirty = TRUE;*/
/*}*/

void ddfs_index_update(Fingerprint* finger, ContainerId id){
    unsigned long hashlen = sizeof(Fingerprint);
    MYSQL_BIND param[3];
    memset(param, 0, sizeof(param));
    param[0].buffer_type = MYSQL_TYPE_BLOB;
    param[0].buffer = finger;
    param[0].buffer_length = hashlen;
    param[0].length = &hashlen;
    param[1].buffer_type = MYSQL_TYPE_LONG;
    param[1].buffer = &id;
    param[2].buffer_type = MYSQL_TYPE_LONG;
    param[2].buffer = &id;

    if(mysql_stmt_bind_param(insert_stmt, param)){
        printf("%s, %d: failed to update index! %s\n",__FILE__,__LINE__, mysql_stmt_error(insert_stmt));
    }
    if(mysql_stmt_execute(insert_stmt)!=0){
        printf("%s, %d: failed to update index! %s\n",__FILE__,__LINE__, mysql_stmt_error(insert_stmt));
    }

    insert_word(filter, (char*)finger, sizeof(Fingerprint));
    dirty = TRUE;
}
