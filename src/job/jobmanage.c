/*
 * jobmanage.c
 *
 *  Created on: May 22, 2012
 *      Author: fumin
 */

#include "../global.h"
#include "jobmanage.h"

extern char working_path[];

static int job_count = 0;
static char jobdir[256] = "";
static char job_count_file[256];
static char job_file[256];

int init_jobmanage() {
	if (strlen(jobdir) == 0) {
		strcpy(jobdir, working_path);
		strcat(jobdir, "/jobs/");
	}

	/*
	 * init job_count
	 */
	strcpy(job_count_file, jobdir);
	strcat(job_count_file, "job_count");
	int fd;
	if ((fd = open(job_count_file, O_CREAT | O_RDWR, S_IRWXU)) <= 0) {
		printf("Can not open job_count_file\n");
		return FAILURE;
	}

	char buf[4];
	if (read(fd, buf, 4) == 4) {
		job_count = *(int*) buf;
	} else {
		job_count = 0;
		lseek(fd, 0, SEEK_SET);
		write(fd, &job_count, 4);
	}
	close(fd);

	strcpy(job_file, jobdir);
	strcat(job_file, "job%d");

	return SUCCESS;
}

int get_next_job_id() {
	int jobid = job_count++;
	int fd;
	if ((fd = open(job_count_file, O_RDWR)) <= 0) {
		printf("Can not write job_count_file\n");
		return FAILURE;
	}
	write(fd, &job_count, 4);
	close(fd);
	return jobid;
}

JobVolume* create_job_volume(int id) { //backup call it
	JobVolume* volume = (JobVolume*) malloc(sizeof(JobVolume));

	volume->job.job_id = id;
	char path[256];

	sprintf(path, job_file, id);
	strcat(path, ".meta");
	if ((volume->job_meta_file = fopen(path, "w+")) <= 0) {
		printf("Can not create job.meta!\n");
		free(volume);
		return NULL;
	}

	sprintf(path, job_file, id);
	strcat(path, ".data");
	if ((volume->job_data_file = fopen(path, "w+")) <= 0) {
		printf("Can not create job.data!\n");
		fclose(volume->job_meta_file);
		free(volume);
		return NULL;
	}

	sprintf(path, job_file, id);
	strcat(path, ".seed");
	if ((volume->job_seed_file = fopen(path, "w+")) <= 0) {
		printf("Can not create job.seed!\n");
		fclose(volume->job_meta_file);
		fclose(volume->job_data_file);
		free(volume);
		return NULL;
	}

	volume->job.is_del = TRUE; //before job finished, set is_del
	memset(volume->job.backup_path, 0, 200);
	volume->job.file_num = 0;
	volume->job.chunk_num = 0;
	update_job_volume_des(volume);

	return volume;
}

int update_job_volume_des(JobVolume* volume) {
	fseek(volume->job_meta_file, 0, SEEK_SET);
	fwrite(&volume->job.job_id, sizeof(volume->job.job_id), 1,
			volume->job_meta_file);
	fwrite(&volume->job.is_del, sizeof(volume->job.is_del), 1,
			volume->job_meta_file);
	fwrite(&volume->job.backup_path, sizeof(volume->job.backup_path), 1,
			volume->job_meta_file);
	fwrite(&volume->job.file_num, sizeof(volume->job.file_num), 1,
			volume->job_meta_file);
	fwrite(&volume->job.chunk_num, sizeof(volume->job.chunk_num), 1,
			volume->job_meta_file);
	return SUCCESS;
}

int load_job_volume_des(JobVolume* volume) {
	fseek(volume->job_meta_file, 0, SEEK_SET);
	fread(&volume->job.job_id, sizeof(volume->job.job_id), 1,
			volume->job_meta_file);
	fread(&volume->job.is_del, sizeof(volume->job.is_del), 1,
			volume->job_meta_file);
	fread(&volume->job.backup_path, sizeof(volume->job.backup_path), 1,
			volume->job_meta_file);
	fread(&volume->job.file_num, sizeof(volume->job.file_num), 1,
			volume->job_meta_file);
	fread(&volume->job.chunk_num, sizeof(volume->job.chunk_num), 1,
			volume->job_meta_file);
	return SUCCESS;
}

BOOL is_job_existed(int id) {
	char path[256];
	sprintf(path, job_file, id);
	strcat(path, ".meta");
	if (access(path, 0) == 0) {
		printf("job_id has already been existed");
		return TRUE;
	}
	return FALSE;
}

JobVolume* open_job_volume(int id) { //restore call it
	JobVolume* volume = (JobVolume*) malloc(sizeof(JobVolume));

	volume->job.job_id = id;
	char path[256];
	sprintf(path, job_file, id);
	strcat(path, ".meta");
	if ((volume->job_meta_file = fopen(path, "r")) <= 0) {
		printf("%s, %d:Can not open job_volume!\n", __FILE__, __LINE__);
		free(volume);
		return NULL;
	}

	sprintf(path, job_file, id);
	strcat(path, ".data");
	if ((volume->job_data_file = fopen(path, "r")) <= 0) {
		printf("%s, %d:Can not open job_volume!\n", __FILE__, __LINE__);
		fclose(volume->job_meta_file);
		free(volume);
		return NULL;
	}

	sprintf(path, job_file, id);
	strcat(path, ".seed");
	if ((volume->job_seed_file = fopen(path, "r")) <= 0) {
		printf("%s, %d:Can not open job_volume!\n", __FILE__, __LINE__);
		fclose(volume->job_meta_file);
		fclose(volume->job_data_file);
		free(volume);
		return NULL;
	}

	load_job_volume_des(volume);
	if (volume->job.job_id != id) {
		printf("%s, %d:unmatched job id!\n", __FILE__, __LINE__);
		fclose(volume->job_meta_file);
		fclose(volume->job_data_file);
		fclose(volume->job_seed_file);
		free(volume);
		return NULL;
	}

	return volume;
}

int close_job_volume(JobVolume* volume) {
	fclose(volume->job_meta_file);
	fclose(volume->job_data_file);
	fclose(volume->job_seed_file);
	free(volume);
	return SUCCESS;
}

int jvol_append_meta(JobVolume *volume, Recipe* recipe) {
	int count = fwrite(&recipe->fileindex, sizeof(recipe->fileindex), 1,
			volume->job_meta_file);
	int filename_length = strlen(recipe->filename);
	count += fwrite(&filename_length, 4, 1, volume->job_meta_file);
	count += fwrite(recipe->filename, strlen(recipe->filename), 1,
			volume->job_meta_file);
	count += fwrite(&recipe->chunknum, sizeof(recipe->chunknum), 1,
			volume->job_meta_file);
	count += fwrite(&recipe->filesize, sizeof(recipe->filesize), 1,
			volume->job_meta_file);
	if (count != 5) {
		printf("%s, %d:failed to append meta.\n", __FILE__, __LINE__);
		return FAILURE;
	}
	return SUCCESS;
}

/*
 * It seems that writing a small piece of buffer each time has little impact on throughput.
 */
int jvol_append_fingerchunk(JobVolume *volume, FingerChunk* fchunk) {
	int count = fwrite(&fchunk->fingerprint, sizeof(Fingerprint), 1,
			volume->job_data_file);
	count += fwrite(&fchunk->container_id, sizeof(fchunk->container_id), 1,
			volume->job_data_file);
	count += fwrite(&fchunk->length, sizeof(fchunk->length), 1,
			volume->job_data_file);
	char valid = '\t';
	count += fwrite(&valid, sizeof(valid), 1, volume->job_data_file);
	if (count != 4) {
		printf("%s, %d:failed to append fingerchunk.\n", __FILE__, __LINE__);
		return FAILURE;
	}
	return SUCCESS;
}

FingerChunk* jvol_read_fingerchunk(JobVolume *volume) {
	FingerChunk *fchunk = (FingerChunk*) malloc(sizeof(FingerChunk));
	int count = fread(&fchunk->fingerprint, sizeof(Fingerprint), 1,
			volume->job_data_file);
	count += fread(&fchunk->container_id, sizeof(fchunk->container_id), 1,
			volume->job_data_file);
	count += fread(&fchunk->length, sizeof(fchunk->length), 1,
			volume->job_data_file);
	char valid;
	count += fread(&valid, sizeof(valid), 1, volume->job_data_file);
	if (count != 4 || valid != '\t') {
		printf("%s, %d:failed to read fingerchunk.\n", __FILE__, __LINE__);
		free(fchunk);
		return NULL;
	}
	fchunk->next = 0;
	return fchunk;
}

int jvol_append_seed(JobVolume *volume, ContainerId cid, int32_t read_size) {
	if (fprintf(volume->job_seed_file, "%d:%d\n", cid, read_size) < 0) {
		printf("%s, %d: failed to append seed\n", __FILE__, __LINE__);
		return FAILURE;
	}
	return SUCCESS;
}

Recipe* read_next_recipe(JobVolume* volume) {
	if (!volume) {
		return NULL;
	}
	Recipe* recipe = recipe_new();

	int filename_length = 0;
	int count = fread(&recipe->fileindex, sizeof(recipe->fileindex), 1,
			volume->job_meta_file);
	count += fread(&filename_length, 4, 1, volume->job_meta_file);
	recipe->filename = (char*) malloc(filename_length + 1);
	count += fread(recipe->filename, filename_length, 1, volume->job_meta_file);
	recipe->filename[filename_length] = 0;
	int32_t chunknum = 0;
	count += fread(&chunknum, sizeof(recipe->chunknum), 1,
			volume->job_meta_file);
	count += fread(&recipe->filesize, sizeof(recipe->filesize), 1,
			volume->job_meta_file);

	if (count != 5) {
		printf("%s, %d: failed to read recipe head\n", __FILE__, __LINE__);
		recipe_free(recipe);
		return NULL;
	}

	return recipe;
}
