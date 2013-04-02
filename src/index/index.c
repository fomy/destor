#include "index.h"
#include "ramindex.h"
#include "ddfsindex.h"
#include "extreme_binning.h"
#include "silo.h"

extern int fingerprint_index_type;

BOOL index_init(){
    switch(fingerprint_index_type){
        case RAM_INDEX:
            puts("index=RAM");
            return ram_index_init();
        case DDFS_INDEX:
            puts("index=DDFS");
            return ddfs_index_init();
        case EXBIN_INDEX:
            puts("index=EXBIN");
            return extreme_binning_init();
        case SILO_INDEX:
            puts("index=SILO");
            return silo_init();
        default:
            printf("%s, %d: Wrong index type!\n",__FILE__,__LINE__);
            return FALSE;
    }
}

void index_destroy(){
    switch(fingerprint_index_type){
        case RAM_INDEX:
            ram_index_destroy();
            break;
        case DDFS_INDEX:
            ddfs_index_destroy();
            break;
        case EXBIN_INDEX:
            extreme_binning_destroy();
            break;
        case SILO_INDEX:
            silo_destroy();
            break;
        default:
            printf("%s, %d: Wrong index type!\n",__FILE__,__LINE__);
    }
}

/*
 * Call index_search() to obtain the container id of the chunk.
 * It should be called immediately after poping a chunk.
 */
ContainerId index_search(Fingerprint* fingerprint, void* eigenvalue){
    switch(fingerprint_index_type){
        case RAM_INDEX:
            return ram_index_search(fingerprint);
        case DDFS_INDEX:
            return ddfs_index_search(fingerprint);
        case EXBIN_INDEX:
            return extreme_binning_search(fingerprint, eigenvalue);
        case SILO_INDEX:
            return silo_search(fingerprint, eigenvalue);
        default:
            printf("%s, %d: Wrong index type!\n",__FILE__,__LINE__);
            return TMP_CONTAINER_ID;
    }
}

/*
 * Update index.
 * It should be called before pushing a chunk into fchunk_queue in filter phase.
 * The update parameter indicates whether the container_id is new.
 */
void index_update(Fingerprint* fingerprint, ContainerId container_id, 
        void* feature, BOOL update){
    /* The update determines wheter update except in SILO */
    switch(fingerprint_index_type){
        case RAM_INDEX:
            if(update){
                ram_index_update(fingerprint, container_id);
            }
            break;
        case DDFS_INDEX:
            if(update){
                ddfs_index_update(fingerprint, container_id);
            }
            break;
        case EXBIN_INDEX:
            if(update){
                extreme_binning_update(fingerprint, container_id, feature);
            }
            break;
        case SILO_INDEX:
            silo_update(fingerprint, container_id, feature);
            break;
        default:
            printf("%s, %d: Wrong index type!\n",__FILE__,__LINE__);
    }
}
