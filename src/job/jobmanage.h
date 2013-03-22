/*
 * jobmanage.h
 *
 *  Created on: May 22, 2012
 *      Author: fumin
 */

#ifndef JOBMANAGE_H_
#define JOBMANAGE_H_

//#define JOB_DES_SIZE 212
//#define FILE_DES_SIZE 208

#include "../dedup.h"
#include "recipe.h"

typedef struct job{ //id, isDel, path,num,len

	int32_t job_id;
	BOOL is_del;
	char backup_path[200];
	int32_t file_num;
    int64_t chunk_num;

	struct job *next_job;
} Job;

typedef struct job_volume {
	Job job;

    FILE *job_meta_file;
    FILE *job_data_file;
    FILE *job_seed_file;
} JobVolume;

extern int init_jobmanage();
extern int get_next_job_id();
extern JobVolume* create_job_volume(int);
extern int update_job_volume_des(JobVolume* volume);
extern int load_job_volume_des(JobVolume* volume);
extern BOOL is_job_existed(int id);
extern JobVolume* open_job_volume(int id);
extern int close_job_volume(JobVolume* volume);

extern int jvol_append_fingerchunk(JobVolume*, FingerChunk*);
extern FingerChunk* jvol_read_fingerchunk(JobVolume*);
extern int jvol_append_meta(JobVolume*, Recipe*);
extern int jvol_append_seed(JobVolume *psVolume, ContainerId cid, int32_t read_size);
Recipe* read_next_recipe(JobVolume*);
#endif /* JOBMANAGE_H_ */
