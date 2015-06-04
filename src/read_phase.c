#include "destor.h"
#include "jcr.h"
#include "backup.h"

static pthread_t read_t;

static void read_file(sds path) {
	static unsigned char buf[DEFAULT_BLOCK_SIZE];

	sds filename = sdsdup(path);

	if (jcr.path[sdslen(jcr.path) - 1] == '/') {
		/* the backup path points to a direcory */
		sdsrange(filename, sdslen(jcr.path), -1);
	} else {
		/* the backup path points to a file */
		int cur = sdslen(filename) - 1;
		while (filename[cur] != '/')
			cur--;
		sdsrange(filename, cur, -1);
	}

	FILE *fp;
	if ((fp = fopen(path, "r")) == NULL) {
		destor_log(DESTOR_WARNING, "Can not open file %s\n", path);
		perror("The reason is");
		exit(1);
	}

	struct chunk *c = new_chunk(sdslen(filename) + 1);
	strcpy(c->data, filename);

	VERBOSE("Read phase: %s", filename);

	SET_CHUNK(c, CHUNK_FILE_START);

	sync_queue_push(read_queue, c);

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	int size = 0;

	while ((size = fread(buf, 1, DEFAULT_BLOCK_SIZE, fp)) != 0) {
		TIMER_END(1, jcr.read_time);

		VERBOSE("Read phase: read %d bytes", size);

		c = new_chunk(size);
		memcpy(c->data, buf, size);

		sync_queue_push(read_queue, c);

		TIMER_BEGIN(1);
	}

	c = new_chunk(0);
	SET_CHUNK(c, CHUNK_FILE_END);
	sync_queue_push(read_queue, c);

	fclose(fp);

	sdsfree(filename);
}

static void find_one_file(sds path) {

	if (strcmp(path + sdslen(path) - 1, "/") == 0) {

		DIR *dir = opendir(path);
		struct dirent *entry;

		while ((entry = readdir(dir)) != 0) {
			/*ignore . and ..*/
			if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
				continue;
			sds newpath = sdsdup(path);
			newpath = sdscat(newpath, entry->d_name);

			struct stat state;
			if (stat(newpath, &state) != 0) {
				WARNING("The file %s does not exist! ignored!", newpath);
				return;
			}

			if (S_ISDIR(state.st_mode)) {
				assert(strcmp(newpath + sdslen(newpath) - 1, "/") != 0);
				newpath = sdscat(newpath, "/");
			}

			find_one_file(newpath);

			sdsfree(newpath);
		}

		closedir(dir);
	} else {
		read_file(path);
	}
}

static void* read_thread(void *argv) {
	/* Each file will be processed separately */
	find_one_file(jcr.path);
	sync_queue_term(read_queue);
	return NULL;
}

void start_read_phase() {
    /* running job */
    jcr.status = JCR_STATUS_RUNNING;
	read_queue = sync_queue_new(10);
	pthread_create(&read_t, NULL, read_thread, NULL);
}

void stop_read_phase() {
	pthread_join(read_t, NULL);
	NOTICE("read phase stops successfully!");
}

