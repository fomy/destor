#include "destor.h"
#include "jcr.h"
#include "backup.h"

static pthread_t read_t;

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

	struct chunk *c = new_chunk(sdslen(filename) + 1);
	strcpy(c->data, filename);
	SET_CHUNK(c, CHUNK_FILE_START);

	sync_queue_push(read_queue, c);

	TIMER_DECLARE(b, e);
	TIMER_BEGIN(b);
	int size = 0;

	while ((size = fread(buf, DEFAULT_BLOCK_SIZE, 1, fp)) == 0) {
		TIMER_END(jcr.read_time, b, e);

		c = new_chunk(size);
		memcpy(c->data, buf, size);

		sync_queue_push(read_queue, c);

		TIMER_BEGIN(b);
	}

	c = new_chunk(0);
	SET_CHUNK(c, CHUNK_FILE_END);
	sync_queue_push(read_queue, c);

	fclose(fp);

	sdsfree(filename);
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
	sync_queue_term(read_queue);
	return NULL;
}

void start_read_phase() {
	read_queue = sync_queue_new(10);
	pthread_create(&read_t, NULL, read_thread, NULL);
}

void stop_read_phase() {
	pthread_join(read_t, NULL);
}

