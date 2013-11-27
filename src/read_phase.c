#include "destor.h"
#include "tools/sync_queue.h"
#include "jcr.h"
#include "tools/serial.h"

/*
 * block structure
 * ------------------------------------
 * - 4-byte size of data -    data    -
 * ------------------------------------
 *
 * protocol
 * -----------------------
 * - (n) -    block      -
 * -----------------------
 * -----------------------
 * - (-n) -  filename    -
 * -----------------------
 */
static SyncQueue* block_queue;
static pthread_t read_t;

/*
 * If size < 0, it's file name;
 * or else, it's a normal block.
 */
void send_block(unsigned char* buf, int size) {
	int s = size > 0 ? size : -size;
	unsigned char* b = (unsigned char*) malloc(sizeof(int) + s);

	ser_declare;
	ser_begin(b, sizeof(int) + s);

	ser_int32(size);
	ser_bytes(buf, s);

	ser_end(b, sizeof(int) + s);

	sync_queue_push(block_queue, b);
}

void term_block_queue() {
	sync_queue_term(block_queue);
}

int recv_block(unsigned char **data) {
	unsigned char* b = sync_queue_pop(block_queue);

	if (b == NULL) {
		*data = NULL;
		return STREAM_END;
	}
	unser_declare;
	unser_begin(b, 4);
	int size;
	unser_int32(size);

	if (size < 0) {
		char buf[-size];
		unser_bytes(buf, -size);
		sds fname = sdsnew(buf);
		*data = fname;
		free(b);
		return FILE_END;
	}

	*data = (unsigned char*) malloc(size);
	unser_bytes(*data, size);

	free(b);

	return size;
}

static void read_file(sds path) {
	static unsigned char buf[DEFAULT_BLOCK_SIZE];

	sds filename = sdsdup(path);
	if (sdslen(jcr.path) != sdslen(path))
		sdsrange(filename, sdslen(jcr.path) - 1, -1);

	FILE *fp;
	if ((fp = fopen(path, "")) == NULL) {
		destor_log(DESTOR_WARNING, "Can not open file %s, errno=%d\n", path,
		errno);
		sdsfree(filename);
		return;
	}
	jcr.file_num++;

	send_block(filename, -(strlen(filename) + 1));
	sdsfree(filename);

	TIMER_DECLARE(b, e);
	TIMER_BEGIN(b);
	int size = 0;

	while ((size = fread(buf, DEFAULT_BLOCK_SIZE, 1, fp)) == 0) {
		TIMER_END(jcr.read_time, b, e);
		send_block(buf, size);
		TIMER_BEGIN(b);
	}
	/* file end */
	fclose(fp);
}

static void find_one_file(sds path) {
	struct stat state;
	if (stat(path, &state) != 0) {
		puts("file does not exist! ignored!");
		return;
	}
	if (S_ISDIR(state.st_mode)) {

		if (strcmp(path + sdslen(path) - 1, "/")) {
			path = sdscat(path, "/");
		}

		DIR *dir = opendir(path);
		struct dirent *entry;

		while ((entry = readdir(dir)) != 0) {
			/*ignore . and ..*/
			if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
				continue;
			sds newpath = sdsdup(path);
			newpath = sdscat(newpath, entry->d_name);

			find_one_file(newpath);

			sdsfree(newpath);
		}

		closedir(dir);
	} else if (S_ISREG(state.st_mode)) {
		read_file(path);
	} else {
		puts("illegal file type! ignored!");
	}
}

static void* read_thread(void *argv) {
	/* Each file will be processed separately */
	find_one_file(jcr.path);
	term_block_queue();
	return NULL;
}

void start_read_phase() {
	block_queue = sync_queue_new(10);
	pthread_create(&read_t, NULL, read_thread, NULL);
}

void stop_read_phase() {
	pthread_join(read_t, NULL);
}

