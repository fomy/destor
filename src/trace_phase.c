#include "destor.h"
#include "jcr.h"
#include "backup.h"

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

void make_trace(char* path) {
	init_jcr(path);

	sds trace_file = sdsnew(path);

	char *p = trace_file + sdslen(trace_file) - 1;
	while (*p == '/')
		--p;
	*(p + 1) = 0;
	sdsupdatelen(trace_file);

	trace_file = sdscat(trace_file, ".trace");
	NOTICE("output to %s", trace_file);

	start_read_phase();
	start_chunk_phase();
	start_hash_phase();

	unsigned char code[41];

	FILE *fp = fopen(trace_file, "w");
	while (1) {
		struct chunk *c = sync_queue_pop(hash_queue);

		if (c == NULL) {
			break;
		}

		if (CHECK_CHUNK(c, CHUNK_FILE_START)) {
			destor_log(DESTOR_NOTICE, c->data);
			fprintf(fp, "file start %zd\n", strlen(c->data));
			fprintf(fp, "%s\n", c->data);

		} else if (CHECK_CHUNK(c, CHUNK_FILE_END)) {
			fprintf(fp, "file end\n");
		} else {
			hash2code(c->fp, code);
			code[40] = 0;
			fprintf(fp, "%s %d\n", code, c->size);
		}
		free_chunk(c);
	}

	fprintf(fp, "stream end");
	fclose(fp);

}

static pthread_t trace_t;

static void* read_trace_thread(void *argv) {

	FILE *trace_file = fopen(jcr.path, "r");
	char line[128];

	while (1) {
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		fgets(line, 128, trace_file);
		TIMER_END(1, jcr.read_time);

		if (strcmp(line, "stream end") == 0) {
			sync_queue_term(trace_queue);
			break;
		}

		struct chunk* c;

		TIMER_BEGIN(1),

		assert(strncmp(line, "file start ", 11) == 0);
		int filenamelen;
		sscanf(line, "file start %d", &filenamelen);

		/* An additional '\n' is read */
		c = new_chunk(filenamelen + 2);
		fgets(c->data, filenamelen + 2, trace_file);
		c->data[filenamelen] = 0;
		VERBOSE("Read trace phase: %s", c->data);

		SET_CHUNK(c, CHUNK_FILE_START);

		TIMER_END(1, jcr.read_time);

		sync_queue_push(trace_queue, c);

		TIMER_BEGIN(1);
		fgets(line, 128, trace_file);
		while (strncmp(line, "file end", 8) != 0) {
			c = new_chunk(0);

			char code[41];
			strncpy(code, line, 40);
			code2hash(code, c->fp);

			c->size = atoi(line + 41);

			TIMER_END(1, jcr.read_time);
			sync_queue_push(trace_queue, c);
			TIMER_BEGIN(1),

			fgets(line, 128, trace_file);
		}

		c = new_chunk(0);
		SET_CHUNK(c, CHUNK_FILE_END);
		sync_queue_push(trace_queue, c);
	}

	fclose(trace_file);
	return NULL;
}

/* fsl/read_fsl_trace.c */
extern void* read_fsl_trace(void *argv);

void start_read_trace_phase() {
    /* running job */
    jcr.status = JCR_STATUS_RUNNING;
	trace_queue = sync_queue_new(100);
    if(destor.trace_format == TRACE_DESTOR)
	    pthread_create(&trace_t, NULL, read_trace_thread, NULL);
    else if(destor.trace_format == TRACE_FSL)
	    pthread_create(&trace_t, NULL, read_fsl_trace, NULL);
    else {
		NOTICE("Invalid trace format");
		exit(1);
    }
}

void stop_read_trace_phase() {
	pthread_join(trace_t, NULL);
	NOTICE("read trace phase stops successfully!");
}
