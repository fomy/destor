/*
 * server.c
 *
 *  Created on: May 24, 2012
 *      Author: fumin
 */

#include "destor.h"
#include "jcr.h"
#include "storage/containerstore.h"

extern void do_backup(char *path);
extern void do_delete(int revision);
extern void do_restore(int revision, char *path);
extern void make_trace(char *raw_files);

extern int load_config();

/* : means argument is required.
 * :: means argument is required and no space.
 */
const char * const short_options = "sr::d::t::h";

struct option long_options[] = { { "restore", 1, NULL, 'r' }, { "state", 0,
NULL, 's' }, { "cache", 1, NULL, 'c' }, { "cache-size", 1, NULL, 'C' }, {
		"help", 0, NULL, 'h' }, { NULL, 0, NULL, 0 } };

void usage() {
	puts("GENERAL USAGE");
	puts("\tstart a backup job");
	puts("\t\tdestor /path/to/data");

	puts("\tstart a restore job");
	puts("\t\tdestor -r<JOB_ID> /path/to/restore");

	puts("\tsimulate deleting backups before JOB_ID.");
	puts("\t\tdestor -d<JOB_ID>");

	puts("\tprint state of destor");
	puts("\t\t./destor -s");

	puts("\tprint this");
	puts("\t\t./destor -h");

	puts("\tMake a trance");
	puts("\t\t./destor -t /path/to/data");

	/*puts("OPTIONS");
	 puts("\t--restore or -r");
	 puts("\t\tA restore job, which required a job id following the option.");
	 puts("\t--state or -s");
	 puts("\t\tprint the state of destor.");
	 puts("\t--help or -h");
	 puts("\t\tprint this.");
	 puts("\t--index=[RAM|DDFS|EXBIN|SILO|SAMPLE|SPARSE|SEGBIN]");
	 puts(
	 "\t\tAssign fingerprint index type. It now support RAM, DDFS, EXBIN, SILO, SAMPLE, and BLC.");
	 puts("\t--cache=[LRU|ASM|OPT]");
	 puts("\t\tAssign read cache type. IT now support LRU, OPT.");
	 puts("\t--cache_size=[number of containers]");
	 puts("\t\tAssign read cache size, e.g. --cache_size=100.");
	 puts("\t--rewrite=[NO|CFL|CBR|CAP|HBR]");
	 puts(
	 "\t\tAssign rewrite algorithm type. It now support NO, CFL, CBR, CAP, HBR.");
	 puts("\t--cfl_p=[p in CFL]");
	 puts("\t\tSet p parameter in CFL, e.g. --cfl_p=3.");
	 puts("\t--rewrite_limit=[rewrite limit for CBR]");
	 puts("\t\tSet rewrite_limit, e.g. --rewrite_limit=0.05.");
	 puts("\t--min_utility=[minimal rewrite utility for CBR]");
	 puts("\t\tSet minimal_rewrite_utility, e.g. --min_utility=0.7.");
	 puts("\t--window_size=[size of the slide window for OPT]");
	 puts(
	 "\t\tSet size of slide window for OPT cache, e.g. --window_size=1024000.");
	 puts("\t--stream_context_size=[size (MB) of the stream context for CBR]");
	 puts("\t\tSet stream_context_size in CBR, e.g. --stream_context_size=16.");
	 puts("\t--capping_t=[T for CAP]");
	 puts("\t\tSet T for capping, e.g. --T=16.");
	 puts("\t--capping_segment_size=[size (MB) of segment in CAP]");
	 puts("\t\tSet segment size for capping, e.g. --capping_segment_size=16.");
	 puts("\t--enable_hbr");
	 puts("\t\tenable HBR.");
	 puts("\t--enable_cache_filter");
	 puts("\t\tenable cache filter.");
	 puts("\t--simulation=[NO|RECOVERY|APPEND|ALL]");
	 puts("\t\tSelect the simulation level.");
	 puts(
	 "\t\tThere are four simulation levels, and each level simulates more phases from NO to ALL.");*/
	exit(0);
}

void destor_log(int level, const char *fmt, ...) {
	va_list ap;
	char msg[DESTOR_MAX_LOGMSG_LEN];

	if ((level & 0xff) < destor.verbosity)
		return;

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	fprintf(stdout, "%s\n", msg);
}

void check_simulation_level(int last_level, int current_level) {
	if ((last_level <= SIMULATION_RESTORE && current_level >= SIMULATION_APPEND)
			|| (last_level >= SIMULATION_APPEND
					&& current_level <= SIMULATION_RESTORE)) {
		fprintf(stderr, "Conflicting simualtion level!\n");
		exit(1);
	}
}

void destor_start() {

	/* Init */
	destor.working_directory = sdsnew("/home/data/working/");
	destor.simulation_level = SIMULATION_NO;
	destor.verbosity = DESTOR_WARNING;

	destor.chunk_algorithm = CHUNK_RABIN;
	destor.chunk_max_size = 65536;
	destor.chunk_min_size = 1024;
	destor.chunk_avg_size = 8192;

	destor.restore_cache[0] = RESTORE_CACHE_LRU;
	destor.restore_cache[1] = 1024;
	destor.restore_opt_window_size = 1000000;

	destor.index_category[0] = INDEX_CATEGORY_NEAR_EXACT;
	destor.index_category[1] = INDEX_CATEGORY_PHYSICAL_LOCALITY;
	destor.index_specific = INDEX_SPECIFIC_SAMPLED;
	/* in number of containers, for DDFS/ChunkStash/BLC/Sampled Index. */
	destor.index_container_cache_size = 4096;

	destor.index_segment_algorithm[0] = INDEX_SEGMENT_FIXED;
	destor.index_segment_algorithm[1] = 128;
	destor.index_feature_method[0] = INDEX_FEATURE_UNIFORM;
	destor.index_feature_method[1] = 1;
	destor.index_segment_selection_method[0] = INDEX_SEGMENT_SELECT_ALL;
	destor.index_segment_selection_method[1] = 1;
	destor.index_segment_prefech = 0;
	destor.index_segment_cache_size = 0;

	destor.rewrite_algorithm[0] = REWRITE_NO;
	destor.rewrite_algorithm[1] = 1024;

	/* for History-Aware Rewriting (HAR) */
	destor.rewrite_enable_har = 0;
	destor.rewrite_har_utilization_threshold = 0.5;

	/* for Cache-Aware Filter */
	destor.rewrite_enable_cache_aware = 0;

	load_config();

	sds stat_file = sdsdup(destor.working_directory);
	stat_file = sdscat(stat_file, "/destor.stat");

	FILE *fp;
	if ((fp = fopen(stat_file, "r"))) {

		fread(&destor.chunk_num, 8, 1, fp);
		fread(&destor.stored_chunk_num, 8, 1, fp);

		fread(&destor.data_size, 8, 1, fp);
		fread(&destor.stored_data_size, 8, 1, fp);

		fread(&destor.zero_chunk_num, 8, 1, fp);
		fread(&destor.zero_chunk_size, 8, 1, fp);

		fread(&destor.rewritten_chunk_num, 8, 1, fp);
		fread(&destor.rewritten_chunk_size, 8, 1, fp);

		int last_level;
		fread(&last_level, 4, 1, fp);
		check_simulation_level(last_level, destor.simulation_level);

		fclose(fp);
	} else {
		destor.chunk_num = 0;
		destor.stored_chunk_num = 0;
		destor.data_size = 0;
		destor.stored_data_size = 0;
		destor.zero_chunk_num = 0;
		destor.zero_chunk_size = 0;
		destor.rewritten_chunk_num = 0;
		destor.rewritten_chunk_size = 0;
	}

	sdsfree(stat_file);
}

void destor_shutdown() {
	sds stat_file = sdsdup(destor.working_directory);
	stat_file = sdscat(stat_file, "/destor.stat");

	FILE *fp;
	if ((fp = fopen(stat_file, "w")) == 0) {
		destor_log(DESTOR_WARNING, "Fatal error, can not open destor.stat!");
		exit(1);
	}

	fwrite(&destor.chunk_num, 8, 1, fp);
	fwrite(&destor.stored_chunk_num, 8, 1, fp);

	fwrite(&destor.data_size, 8, 1, fp);
	fwrite(&destor.stored_data_size, 8, 1, fp);

	fwrite(&destor.zero_chunk_num, 8, 1, fp);
	fwrite(&destor.zero_chunk_size, 8, 1, fp);

	fwrite(&destor.rewritten_chunk_num, 8, 1, fp);
	fwrite(&destor.rewritten_chunk_size, 8, 1, fp);

	fwrite(&destor.simulation_level, 4, 1, fp);

	fclose(fp);
	sdsfree(stat_file);
}

void destor_stat() {
	printf("=== destor stat ===");

	printf("the number of chunks: %ld\n", destor.chunk_num);
	printf("the number of stored chunks: %ld\n", destor.stored_chunk_num);

	printf("the size of data: %ld\n", destor.data_size);
	printf("the size of stored data: %ld\n", destor.stored_data_size);
	printf("the size of saved data: %ld\n",
			destor.data_size - destor.stored_data_size);

	printf("deduplication ratio: %.4f, %.4f\n",
			(destor.data_size - destor.stored_data_size)
					/ (double) destor.data_size,
			((double) destor.data_size) / (destor.stored_data_size));

	printf("the number of zero chunks: %ld\n", destor.zero_chunk_num);
	printf("the size of zero chunks: %ld\n", destor.zero_chunk_size);

	printf("the number of rewritten chunks: %ld\n", destor.rewritten_chunk_num);
	printf("the size of rewritten chunks: %ld\n", destor.rewritten_chunk_size);
	printf("rewrite ratio: %.4f\n",
			destor.rewritten_chunk_size / (double) destor.data_size);

	if (destor.simulation_level == SIMULATION_NO)
		printf("simulation level is %s\n", "NO");
	else if (destor.simulation_level == SIMULATION_RESTORE)
		printf("simulation level is %s\n", "RESTORE");
	else if (destor.simulation_level == SIMULATION_APPEND)
		printf("simulation level is %s\n", "APPEND");
	else if (destor.simulation_level == SIMULATION_ALL)
		printf("simulation level is %s\n", "ALL");
	else {
		printf("Invalid simulation level.\n");
	}

	printf("=== destor stat ===");
	exit(0);
}

int main(int argc, char **argv) {

	destor_start();

	int job = DESTOR_BACKUP;
	int revision = -1;
	sds path = sdsempty();

	int opt = 0;
	while ((opt = getopt_long(argc, argv, short_options, long_options, NULL))
			!= -1) {
		switch (opt) {
		case 'r':
			job = DESTOR_RESTORE;
			revision = atoi(optarg);
			break;
		case 's':
			destor_stat();
			break;
		case 't':
			job = DESTOR_MAKE_TRACE;
			break;
		case 'd':
			job = DESTOR_DELETE;
			revision = atoi(optarg);
			break;
		case 'c':
			if (strcasecmp(optarg, "lru") == 0)
				destor.restore_cache[0] = RESTORE_CACHE_LRU;
			else if (strcasecmp(optarg, "opt") == 0)
				destor.restore_cache[0] = RESTORE_CACHE_OPT;
			else if (strcasecmp(optarg, "asm") == 0)
				destor.restore_cache[0] = RESTORE_CACHE_ASM;
			else {
				fprintf(stderr, "Invalid restore cache");
				usage();
			}
			break;
		case 'C':
			destor.restore_cache[1] = atoi(optarg);
			break;
		case 'h':
			usage();
			break;
		default:
			return 0;
		}
	}

	switch (job) {
	case DESTOR_BACKUP:
		if (argc > optind) {
			path = sdscpy(path, argv[optind]);
		} else {
			fprintf(stderr, "backup job needs a protected path!\n");
			usage();
		}
		init_recipe_store();
		init_container_store();

		do_backup(path);

		close_container_store();
		close_recipe_store();
		break;
	case DESTOR_RESTORE:
		if (revision < 0) {
			fprintf(stderr, "A job id is required!\n");
			usage();
		}
		if (argc > optind) {
			path = sdscpy(path, argv[optind]);
		} else {
			fprintf(stderr, "A target directory is required!\n");
			usage();
		}
		init_recipe_store();
		init_container_store();

		do_restore(revision, path[0] == 0 ? 0 : path);

		close_container_store();
		close_recipe_store();
		break;
	case DESTOR_MAKE_TRACE: {
		if (argc > optind) {
			sdscpy(path, argv[optind]);
		} else {
			fprintf(stderr, "A target directory is required!\n");
			usage();
		}

		make_trace(path);
		break;
	}
	case DESTOR_DELETE:
		if (revision < 0) {
			fprintf(stderr, "Invalid job id!");
			usage();
		}
		init_container_store();
		/*do_delete(revision);*/
		close_container_store();
		break;
	default:
		fprintf(stderr, "Invalid job type!\n");
		usage();
	}
	destor_shutdown();

	return 0;
}

struct chunk* new_chunk(int32_t size) {
	struct chunk* ck = (struct chunk*) malloc(sizeof(struct chunk));

	ck->flag = CHUNK_UNIQUE;
	ck->id = TEMPORARY_ID;
	memset(&ck->fp, 0x0, sizeof(fingerprint));
	ck->size = size;

	if (size > 0)
		ck->data = malloc(size);
	else
		ck->data = NULL;

	return ck;
}

void free_chunk(struct chunk* ck) {
	if (ck->data) {
		free(ck->data);
		ck->data = NULL;
	}
	free(ck);
}

struct segment* new_segment() {
	struct segment * s = (struct segment*) malloc(sizeof(struct segment));
	s->chunk_num = 0;
	s->chunks = g_queue_new();
	s->features = NULL;
	return s;
}

void free_segment(struct segment* s, void (*free_data)(void *)) {
	g_queue_free_full(s->chunks, free_data);

	if (s->features)
		g_hash_table_destroy(s->features);

	free(s);
}

gboolean g_fingerprint_equal(fingerprint* fp1, fingerprint* fp2) {
	return !memcmp(fp1, fp2, sizeof(fingerprint));
}

gint g_fingerprint_cmp(fingerprint* fp1, fingerprint* fp2, gpointer user_data) {
	return memcmp(fp1, fp2, sizeof(fingerprint));
}
