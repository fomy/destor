#include "statistic.h"

DestorStat *destor_stat;
extern char working_path[];
char stat_path[40];

int init_destor_stat() {
	destor_stat = (DestorStat*) malloc(sizeof(DestorStat));

	strcpy(stat_path, working_path);
	strcat(stat_path, "destor_stat");

	int fd;
	if ((fd = open(stat_path, O_CREAT | O_RDWR, S_IRWXU)) < 0) {
		puts("Can not open destor_stat file!");
		return FAILURE;
	}
	int flag = 0;
	int n = read(fd, &flag, 4);
	if (n == 4 && flag == 0xabcdabcd) {
		read(fd, &destor_stat->number_of_chunks, 8);

		read(fd, &destor_stat->number_of_dup_chunks, 8);

		read(fd, &destor_stat->data_amount, 8);
		read(fd, &destor_stat->consumed_capacity, 8);
		read(fd, &destor_stat->saved_capacity, 8);

		read(fd, &destor_stat->zero_chunk_count, 4);
		read(fd, &destor_stat->zero_chunk_amount, 8);

		read(fd, &destor_stat->rewritten_chunk_count, 4);
		read(fd, &destor_stat->rewritten_chunk_amount, 8);

		read(fd, &destor_stat->sparse_chunk_count, 4);
		read(fd, &destor_stat->sparse_chunk_amount, 8);

		read(fd, &destor_stat->simulation_level, 4);

	} else {
		destor_stat->number_of_chunks = 0;

		destor_stat->number_of_dup_chunks = 0;

		destor_stat->data_amount = 0;
		destor_stat->consumed_capacity = 0;
		destor_stat->saved_capacity = 0;

		destor_stat->zero_chunk_count = 0;
		destor_stat->zero_chunk_amount = 0;

		destor_stat->rewritten_chunk_count = 0;
		destor_stat->rewritten_chunk_amount = 0;

		destor_stat->sparse_chunk_count = 0;
		destor_stat->sparse_chunk_amount = 0;

		destor_stat->simulation_level = SIMULATION_NO;

	}

	close(fd);
	return SUCCESS;
}

int free_destor_stat() {
	int fd;
	if ((fd = open(stat_path, O_CREAT | O_RDWR, S_IRWXU)) == 0) {
		puts("Can not open destor_stat file!");
		return FAILURE;
	}

	int flag = 0xabcdabcd;
	write(fd, &flag, 4);

	write(fd, &destor_stat->number_of_chunks, 8);

	write(fd, &destor_stat->number_of_dup_chunks, 8);

	write(fd, &destor_stat->data_amount, 8);
	write(fd, &destor_stat->consumed_capacity, 8);
	write(fd, &destor_stat->saved_capacity, 8);

	write(fd, &destor_stat->zero_chunk_count, 4);
	write(fd, &destor_stat->zero_chunk_amount, 8);

	write(fd, &destor_stat->rewritten_chunk_count, 4);
	write(fd, &destor_stat->rewritten_chunk_amount, 8);

	write(fd, &destor_stat->sparse_chunk_count, 4);
	write(fd, &destor_stat->sparse_chunk_amount, 8);

	write(fd, &destor_stat->simulation_level, 4);

	close(fd);
	free(destor_stat);
	return SUCCESS;
}

void print_destor_stat() {
	puts("=== the statistics of server ===");

	printf("the number of chunks: %ld\n", destor_stat->number_of_chunks);
	printf("the number of duplicated chunks: %ld\n",
			destor_stat->number_of_dup_chunks);

	printf("the data amount: %ld\n", destor_stat->data_amount);
	printf("consumed capacity for unique chunks: %ld\n",
			destor_stat->consumed_capacity);
	printf("saved capacity due to dedup: %ld\n", destor_stat->saved_capacity);
	printf("dedup efficiency: %.4f, %.4f\n",
			((double) destor_stat->saved_capacity) / destor_stat->data_amount,
			((double) destor_stat->data_amount)
					/ (destor_stat->consumed_capacity));

	printf("zero chunk count: %d\n", destor_stat->zero_chunk_count);
	printf("zero chunk amount: %ld\n", destor_stat->zero_chunk_amount);

	printf("rewritten chunk count: %d\n", destor_stat->rewritten_chunk_count);
	printf("rewritten chunk amount: %ld\n",
			destor_stat->rewritten_chunk_amount);

	printf("sparse chunk count: %d\n", destor_stat->sparse_chunk_count);
	printf("sparse chunk amount: %ld\n", destor_stat->sparse_chunk_amount);

	if (destor_stat->simulation_level == SIMULATION_NO)
		printf("simulation level is %s\n", "NO");
	else if (destor_stat->simulation_level == SIMULATION_RECOVERY)
		printf("simulation level is %s\n", "RECOVERY");
	else if (destor_stat->simulation_level == SIMULATION_APPEND)
		printf("simulation level is %s\n", "APPEND");
	else if (destor_stat->simulation_level == SIMULATION_ALL)
		printf("simulation level is %s\n", "ALL");
	else
		dprint("A wrong simulation level!");

	puts("=== the statistics of server ===");
}

int update_destor_stat() {
	int fd;
	if ((fd = open(stat_path, O_CREAT | O_RDWR, S_IRWXU)) == 0) {
		puts("Can not open destor_stat file!");
		return FAILURE;
	}

	int flag = 0xabcdabcd;
	write(fd, &flag, 4);

	write(fd, &destor_stat->number_of_chunks, 8);

	write(fd, &destor_stat->number_of_dup_chunks, 8);

	write(fd, &destor_stat->data_amount, 8);
	write(fd, &destor_stat->consumed_capacity, 8);
	write(fd, &destor_stat->saved_capacity, 8);

	write(fd, &destor_stat->zero_chunk_count, 4);
	write(fd, &destor_stat->zero_chunk_amount, 8);

	write(fd, &destor_stat->rewritten_chunk_count, 4);
	write(fd, &destor_stat->rewritten_chunk_amount, 8);

	write(fd, &destor_stat->sparse_chunk_count, 4);
	write(fd, &destor_stat->sparse_chunk_amount, 8);

	close(fd);
	return SUCCESS;
}
