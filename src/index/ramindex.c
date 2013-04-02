#include "../global.h"
#include "../dedup.h"

#define INDEX_ITEM_SIZE 24

extern char working_path[];

static char indexpath[256];
/* hash table */
static GHashTable *table;

static BOOL dirty = FALSE;

int ram_index_init() {
    strcpy(indexpath, working_path);
    strcat(indexpath, "index/ramindex.db");
    int fd;
    if ((fd = open(indexpath, O_RDONLY | O_CREAT, S_IRWXU)) <= 0) {
        dprint("Can not open index/ramindex.db!");
        return FALSE;
    }

    struct stat fileInfo;
    stat(indexpath, &fileInfo);
    size_t nFileSize = fileInfo.st_size;

    table = g_hash_table_new_full(g_int64_hash, g_fingerprint_cmp,
            free, free);

    if (nFileSize > 0) {
        uint32_t itemNum;
        if (read(fd, &itemNum, 4) != 4) {
            puts("error!");
        }
        char buf[INDEX_ITEM_SIZE];
        while (itemNum) {
            read(fd, buf, INDEX_ITEM_SIZE);

            Fingerprint* fp = (Fingerprint*) malloc(sizeof(Fingerprint));
            memcpy(fp, buf, 20);
            ContainerId *addr = (ContainerId*) malloc(sizeof(ContainerId));
            memcpy(addr, buf + 20, 4);

            g_hash_table_replace(table, fp, addr);
            --itemNum;
        }
    }
    close(fd);

    dirty = FALSE;
    return TRUE;
}

/*
 * This function is called to write one index item to disk when destroy the whole index.
 * */
static void write_index_item(gpointer key, gpointer value, gpointer fd) {
    int count = 0;
    count += write(*(int*) fd, (Fingerprint*) key, sizeof(Fingerprint));
    count += write(*(int*) fd, ((ContainerId*) value), sizeof(ContainerId));
    if( count != INDEX_ITEM_SIZE){
        printf("%s, %d: write_index_item failure!\n",__FILE__,__LINE__);
    }
    /*return count == INDEX_ITEM_SIZE ? TRUE : FALSE;*/
}

void ram_index_flush() {
    if(dirty == FALSE)
        return;
    int fd;
    if ((fd = open(indexpath, O_WRONLY | O_CREAT, S_IRWXU)) <= 0) {
        printf("Can not open RAMDedup/hash.db!\n");
        return;
    }
    uint32_t itemNum = g_hash_table_size(table);
    write(fd, &itemNum, 4);
    g_hash_table_foreach(table, write_index_item, &fd);

    close(fd);
    dirty = FALSE;
}

void ram_index_destroy() {
    ram_index_flush();
    g_hash_table_destroy(table);
}

ContainerId ram_index_search(Fingerprint *fp) {
    ContainerId* addr = g_hash_table_lookup(table, fp);
    return addr==0?-1:*addr;
}

/*void ram_index_update(Fingerprint* fingers, int32_t fingernum, ContainerId id) {*/
    /*int i = 0;*/
    /*for (; i < fingernum; ++i) {*/
        /*ContainerId *addr = (ContainerId*) malloc(sizeof(ContainerId));*/
        /**addr = id;*/
        /*Fingerprint* fp = (Fingerprint*) malloc(sizeof(Fingerprint));*/
        /*memcpy(fp, fingers[i], sizeof(Fingerprint));*/

        /*g_hash_table_replace(table, fp, addr);*/
    /*}*/
    /*dirty = TRUE;*/
/*}*/

void ram_index_update(Fingerprint* finger, ContainerId id) {
    ContainerId *addr = (ContainerId*) malloc(sizeof(ContainerId));
    *addr = id;
    Fingerprint* fp = (Fingerprint*) malloc(sizeof(Fingerprint));
    memcpy(fp, finger, sizeof(Fingerprint));

    g_hash_table_replace(table, fp, addr);
    dirty = TRUE;
}
