/*
 * destor_dedup.c
 *  
 *  Created on: Dec 9, 2012
 *      Author: fumin
 */

#include "global.h"
#include "index/index.h"

/* server parameters */
char working_path[200] = "/home/data/working/";

/* enable writing data during the recovery */
BOOL enable_writing = TRUE;

/* read cahce parameters */
int read_cache_size = 100;
BOOL enable_data_cache = TRUE;
int read_cache_type = LRU_CACHE;
/* optimal read cache parameter */
int optimal_cache_window_size = 10000;

/* index type*/
int fingerprint_index_type = RAM_INDEX;
/* ddfs_index parameters */
int ddfs_cache_size = 100;

/* filter type */
int rewriting_algorithm = NO_REWRITING;
/* cfl_monitor parameters */
/* also used in container usage monitor */
double container_usage_threshold = 0.7;//should larger than cfl_require
double cfl_require = 0.6;

/* cbr_filter parameters */
double minimal_rewrite_utility = 0.7;
double rewrite_limit = 0.05;
int32_t stream_context_size = 32*1024*1024;
int32_t disk_context_size = 1;//container

/* SiLo parameters */
int32_t silo_segment_size = 2048;//KB
int32_t silo_block_size = 128;//MB

/* capping parameters */
int32_t capping_T = 20;
int32_t capping_segment_size = 20*1024*1024; 

void set_value(char *pname, char *pvalue){
    if(strcmp(pname, "WORKING_PATH") == 0){
        strcpy(working_path, pvalue);
    }
    else if(strcmp(pname, "READ_CACHE_SIZE") == 0){
        read_cache_size = atoi(pvalue);
    }
    else if(strcmp(pname, "READ_CACHE_TYPE") == 0){
        if(strcmp(pvalue, "LRU")==0){
            read_cache_type = LRU_CACHE;
        }else if(strcmp(pvalue, "OPT")==0){
            read_cache_type = OPT_CACHE;
        }else if(strcmp(pvalue, "ASM")==0){
            read_cache_type = ASM_CACHE;
        }else{
            printf("%s, %d: unknown cache type\n",__FILE__,__LINE__);
        }
    }
    /*else if(strcmp(pname, "ENABLE_DATA_CACHE") == 0){*/
        /*enable_data_cache = atoi(pvalue);*/
    /*}*/
    else if(strcmp(pname, "OPTIMAL_CACHE_WINDOW_SIZE") == 0){
        optimal_cache_window_size = atoi(pvalue);
    }
    else if(strcmp(pname, "INDEX_TYPE") == 0){
        if(strcmp(pvalue, "RAM") == 0){
            fingerprint_index_type = RAM_INDEX;
        }else if(strcmp(pvalue, "DDFS") == 0){
            fingerprint_index_type = DDFS_INDEX;
        }else if(strcmp(pvalue, "EXBIN") == 0){
            fingerprint_index_type = EXBIN_INDEX;
        }else if(strcmp(pvalue, "SILO") == 0){
            fingerprint_index_type = SILO_INDEX;
        }else{
            printf("%s, %d: unknown index type\n",__FILE__,__LINE__);
        }
    }
    else if(strcmp(pname, "DDFS_CACHE_SIZE") == 0){
        ddfs_cache_size = atoi(pvalue);
    }
    else if(strcmp(pname, "CFL_REQUIRE") == 0){
        cfl_require = atof(pvalue);
    }
    else if(strcmp(pname, "CONTAINER_USAGE_THRESHOLD") == 0){
        container_usage_threshold = atof(pvalue);
    }
    else if(strcmp(pname, "REWRITE") == 0){
        if(strcmp(pvalue, "NO")==0){
            rewriting_algorithm = NO_REWRITING;
        }else if(strcmp(pvalue, "CFL")==0){
            rewriting_algorithm = CFL_REWRITING;
        }else if(strcmp(pvalue, "CBR") == 0){
            rewriting_algorithm = CBR_REWRITING;
        }else if(strcmp(pvalue, "HBR") == 0){
            rewriting_algorithm = HBR_REWRITING;
        }else if(strcmp(pvalue, "HBR_CBR") == 0){
            rewriting_algorithm = HBR_CBR_REWRITING;
        }else if(strcmp(pvalue, "CAP") == 0){
            rewriting_algorithm = CAP_REWRITING;
        }else if(strcmp(pvalue, "HBR_CAP") == 0){
            rewriting_algorithm = HBR_CAP_REWRITING;
        }else{
            printf("%s, %d: unknown rewriting algorithm\n",__FILE__,__LINE__);
        }
    }
    else if(strcmp(pname, "MINIMAL_REWRITE_UTILITY") == 0){
        minimal_rewrite_utility = atof(pvalue);
    }
    else if(strcmp(pname, "REWRITE_LIMIT") == 0){
        rewrite_limit = atof(pvalue);
    }
    else if(strcmp(pname, "STREAM_CONTEXT_SIZE") == 0){
        stream_context_size = atoi(pvalue)*1024*1024;
    }
    else if(strcmp(pname, "DISK_CONTEXT_SIZE") == 0){
        disk_context_size = atoi(pvalue);
    }
    else if(strcmp(pname, "SILO_SEGMENT_SIZE") == 0){
        silo_segment_size = atoi(pvalue);
    }
    else if(strcmp(pname, "SILO_BLOCK_SIZE") == 0){
        silo_block_size = atoi(pvalue);
    }
    else if(strcmp(pname, "CAP_T") == 0){
        capping_T = atoi(pvalue);
    }
    else if(strcmp(pname, "CAP_SEGMENT_SIZE") == 0){
        capping_segment_size = atoi(pvalue)*1024*1024;
    }
    else{
        printf("%s, %d: no such param name\n",__FILE__,__LINE__);
    }
}

int load_config(){
    FILE *psFile;
    if ((psFile = fopen("destor.config", "r")) == 0) {
        puts("destor.config does not exist!");
        return TRUE;
    }
    char line[50];
    char *pname, *pvalue;
    while (fgets(line, 50, psFile)) {
        switch(line[0]){
            case '#':
            case '\n':
                break;
            default:
                pname = strtok(line, "=\n");
                pvalue = strtok(NULL, "=\n");
                set_value(pname, pvalue);
        }
    }
}
