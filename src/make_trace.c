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
			switch (a) {
			case 'A':
				b = 10;
				break;
			case 'B':
				b = 11;
				break;
			case 'C':
				b = 12;
				break;
			case 'D':
				b = 13;
				break;
			case 'E':
				b = 14;
				break;
			case 'F':
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

			fprintf(trace, "file index = %d\n", file_index);
		}
		if (signal == FILE_END) {
			/* TO-DO */
			fprintf(trace, "FILE END\n");
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

	fprintf(trace, "STREAM END");

	stop_read_phase();
	stop_chunk_phase();
	stop_hash_phase();

	fclose(trace);

}
