#include "global.h"
#include "jcr.h"

extern int start_read_phase(Jcr*);
extern void stop_read_phase();

extern int start_chunk_phase(Jcr*);
extern void stop_chunk_phase();

extern int start_hash_phase(Jcr*);
extern void stop_hash_phase();

extern int recv_hash(Chunk **new_chunk);

extern int simulation_level;

void hash2code(unsigned char hash[20], char code[40]) {
	int i, j, b;
	unsigned char a, c;
	i = 0;
	for (i = 0; i < 20; i++) {
		a = hash[i];
		for (j = 0; j < 2; j++) {
			b = a / 16;
			switch (b) {
			case 10:
				c = 'A';
				break;
			case 11:
				c = 'B';
				break;
			case 12:
				c = 'C';
				break;
			case 13:
				c = 'D';
				break;
			case 14:
				c = 'E';
				break;
			case 15:
				c = 'F';
				break;
			default:
				c = b + 48;
				if (c == 'A' || c == 'B' || c == 'C' || c == 'D' || c == 'E'
						|| c == 'F')
					dprint("not good")
				;
				break;

			}
			code[2 * i + j] = c;
			a = a << 4;
		}
	}
}

void code2hash(unsigned char code[40], unsigned char hash[20]) {
	bzero(hash, 20);
	int i, j;
	unsigned char a, b;
	for (i = 0; i < 20; i++) {
		for (j = 0; j < 2; j++) {
			a = code[2 * i + j];
			switch (a) { //A is equal to a
			case 'A':
				b = 10;
				break;
			case 'a':
				b = 10;
				break;
			case 'B':
				b = 11;
				break;
			case 'b':
				b = 11;
				break;
			case 'C':
				b = 12;
				break;
			case 'c':
				b = 12;
				break;
			case 'D':
				b = 13;
				break;
			case 'd':
				b = 13;
				break;
			case 'E':
				b = 14;
				break;
			case 'e':
				b = 14;
				break;
			case 'F':
				b = 15;
				break;
			case 'f':
				b = 15;
				break;
			default:
				b = a - 48;
				break;
			}
			hash[i] = hash[i] * 16 + b;
		}
	}
}

void make_trace(char* raw_files) {
	Jcr *jcr = new_write_jcr();
	strcpy(jcr->backup_path, raw_files);

	char trace_file[512];
	if (access(raw_files, 4) != 0) {
		puts("This raw file path does not exist or can not be read!");
		return;
	}

	strcpy(trace_file, raw_files);
	struct stat state;
	if (stat(raw_files, &state) != 0) {
		puts("backup path does not exist!");
		return;
	}
	if (S_ISDIR(state.st_mode)) {
		char *p = trace_file + strlen(trace_file) - 1;
		while (*p == '/')
			--p;
		*(p + 1) = 0;
	}

	strcat(trace_file, ".trace");
	printf("output to %s\n", trace_file);
	FILE *trace = fopen(trace_file, "w");

	simulation_level = SIMULATION_NO;

	start_read_phase(jcr);
	start_chunk_phase(jcr);
	start_hash_phase(jcr);

	unsigned char code[41];
	int32_t file_index = 0;
	Recipe *processing_recipe = NULL;

	while (TRUE) {
		Chunk *chunk = NULL;
		int signal = recv_hash(&chunk);
		if (signal == STREAM_END) {
			free_chunk(chunk);
			break;
		}
		if (processing_recipe == 0) {
			processing_recipe = sync_queue_pop(jcr->waiting_files_queue);
			puts(processing_recipe->filename);

			fprintf(trace, "fileindex=%d\n", file_index);
		}
		if (signal == FILE_END) {
			/* TO-DO */
			fprintf(trace, "FILE_END\n");
			close(processing_recipe->fd);
			processing_recipe->fd = -1;

			recipe_free(processing_recipe);
			processing_recipe = 0;
			free_chunk(chunk);

			file_index++;
			/* FILE notion is meaningless for following threads */
			continue;
		}

		hash2code(chunk->hash, code);
		code[40] = 0;
		fprintf(trace, "%s:%d\n", code, chunk->length);

		/* TO-DO */
		processing_recipe->chunknum++;
		processing_recipe->filesize += chunk->length;

		free_chunk(chunk);
	}

	fprintf(trace, "STREAM_END");

	stop_read_phase();
	stop_chunk_phase();
	stop_hash_phase();

	fclose(trace);

}

static SyncQueue* trace_queue;
static pthread_t trace_t;

int recv_trace_chunk(Chunk **new_chunk) {
	Chunk *chunk = sync_queue_pop(trace_queue);
	if (chunk->length == FILE_END) {
		*new_chunk = chunk;
		return FILE_END;
	} else if (chunk->length == STREAM_END) {
		*new_chunk = chunk;
		return STREAM_END;
	}
	*new_chunk = chunk;
	return SUCCESS;
}

static void send_trace_chunk(Chunk* chunk) {
	sync_queue_push(trace_queue, chunk);
}

static void signal_trace_chunk(int signal) {
	Chunk *chunk = allocate_chunk();
	chunk->length = signal;
	sync_queue_push(trace_queue, chunk);
}

static void* read_trace_thread(void *argv) {
	Jcr* jcr = (Jcr*) argv;

	struct stat state;
	if (stat(jcr->backup_path, &state) != 0) {
		puts("The trace does not exist!");
		return NULL ;
	}

	if (!S_ISREG(state.st_mode)) {
		dprint("It is not a file!");
		return NULL ;
	}

	FILE *trace_file = fopen(jcr->backup_path, "r");
	char line[128];

	while (TRUE) {
		TIMER_DECLARE(b, e);
		TIMER_BEGIN(b);
		fgets(line, 128, trace_file);
		TIMER_END(jcr->read_time, b, e);

		if (strncmp(line, "STREAM_END", 10) == 0) {
			signal_trace_chunk(STREAM_END);
			break;
		}

		if (strncmp(line, "fileindex=", 10) == 0) {
			char fileindex[10];
			strncpy(fileindex, line + 10, strlen(line + 10) - 1);
			fileindex[strlen(line + 10) - 1] = 0;

			Recipe *recipe = recipe_new_full(fileindex);
			jcr->file_num++;
			sync_queue_push(jcr->waiting_files_queue, recipe);

			continue;
		}
		if (strncmp(line, "FILE_END", 8) == 0) {
			signal_trace_chunk(FILE_END);
			continue;
		}

		Chunk * new_chunk = allocate_chunk();
		char code[41];
		strncpy(code, line, 40);
		code2hash(code, new_chunk->hash);

		new_chunk->length = atoi(line + 41);

		/* useless */
		new_chunk->data = malloc(new_chunk->length);
		jcr->job_size += new_chunk->length;

		send_trace_chunk(new_chunk);
	}

	fclose(trace_file);

}

int start_read_trace_phase(Jcr *jcr) {
	trace_queue = sync_queue_new(100);
	pthread_create(&trace_t, NULL, read_trace_thread, jcr);
}

void stop_read_trace_phase() {
	pthread_join(trace_t, NULL );
}
