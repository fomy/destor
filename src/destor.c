/*
 * server.c
 *
 *  Created on: May 24, 2012
 *      Author: fumin
 */

#include "global.h"
#include "job/jobmanage.h"
#include "jcr.h"
#include "statistic.h"

extern int backup_server(char *path);
extern int restore_server(int revision, char *path);
extern void make_trace(char *raw_files);

extern int load_config();

extern int simulation_level;
extern BOOL enable_hbr;
extern BOOL enable_cache_filter;
extern int read_cache_type;
extern int read_cache_size;
extern int fingerprint_index_type;
extern int rewriting_algorithm;
extern int optimal_cache_window_size;
extern double cfl_usage_threshold;
extern double hbr_usage_threshold;
extern double minimal_rewrite_utility;
extern double rewrite_limit;
extern int32_t stream_context_size;
extern int32_t capping_T;
extern int32_t capping_segment_size;

#define BACKUP_JOB 1
#define RESTORE_JOB 2
#define STAT_JOB 3
#define HELP_JOB 4 
#define MAKE_TRACE 5

/* : means argument is required.
 * :: means argument is required and no space.
 */
const char * const short_options = "sr::t::h";

struct option long_options[] = { { "restore", 1, NULL, 'r' }, { "state", 0,
		NULL, 's' }, { "index", 1, NULL, 'i' }, { "cache", 1, NULL, 'c' }, {
		"cache_size", 1, NULL, 'C' }, { "rewrite", 1, NULL, 'R' }, { "cfl_p", 1,
		NULL, 'p' }, { "hbr_usage", 1, NULL, 'u' }, { "min_utility", 1, NULL,
		'm' }, { "rewrite_limit", 1, NULL, 'l' }, { "stream_context_size", 1,
		NULL, 'S' }, { "window_size", 1, NULL, 'w' }, { "capping_t", 1, NULL,
		'T' }, { "capping_segment_size", 1, NULL, 'a' }, { "enable_hbr", 0,
		NULL, 'e' }, { "enable_cache_filter", 0, NULL, 'E' }, { "simulation", 1,
		NULL, 'I' }, { "help", 0, NULL, 'h' }, { NULL, 0, NULL, 0 } };

void print_help() {
	puts("GENERAL USAGE");
	puts("\tstart a backup job");
	puts("\t\t./destor <protected_path>");
	puts("\tstart a restore job");
	puts("\t\t./destor -r<JOB_ID> <target_path>");
	puts("\tprint state of destor");
	puts("\t\t./destor -s");
	puts("\tprint this");
	puts("\t\t./destor -h");
	puts("\tMake a trance");
	puts("\t\t./destor -t <input raw file>");

	puts("OPTIONS");
	puts("\t--restore or -r");
	puts("\t\tA restore job, which required a job id following the option.");
	puts("\t--state or -s");
	puts("\t\tprint the state of destor.");
	puts("\t--help or -h");
	puts("\t\tprint this.");
	puts("\t--index=[RAM|DDFS|EXBIN|SILO]");
	puts(
			"\t\tAssign fingerprint index type. It now support RAM, DDFS, EXBIN, SILO.");
	puts("\t--cache=[LRU|ASM|OPT]");
	puts("\t\tAssign read cache type. IT now support LRU, OPT.");
	puts("\t--cache_size=[number of containers]");
	puts("\t\tAssign read cache size, e.g. --cache_size=100.");
	puts("\t--rewrite=[NO|CFL|CBR|CAP|HBR|HBR_CBR|HBR_CAP|HBR_CFL]");
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
			"\t\tThere are four simulation levels, and each level simulates more phases from NO to ALL.");
}

int main(int argc, char **argv) {

	if (load_config() == FAILURE) {
		return 0;
	}

	int job_type = BACKUP_JOB;
	int revision = -1;
	char path[512];
	bzero(path, 512);
	int opt = 0;
	while ((opt = getopt_long(argc, argv, short_options, long_options, NULL ))
			!= -1) {
		switch (opt) {
		case 'r':
			job_type = RESTORE_JOB;
			revision = atoi(optarg);
			break;
		case 's':
			job_type = STAT_JOB;
			break;
		case 't':
			job_type = MAKE_TRACE;
			break;
		case 'i':
			if (strcmp(optarg, "RAM") == 0) {
				fingerprint_index_type = RAM_INDEX;
			} else if (strcmp(optarg, "DDFS") == 0) {
				fingerprint_index_type = DDFS_INDEX;
			} else if (strcmp(optarg, "EXBIN") == 0) {
				fingerprint_index_type = EXBIN_INDEX;
			} else if (strcmp(optarg, "SILO") == 0) {
				fingerprint_index_type = SILO_INDEX;
			} else if (strcmp(optarg, "SPARSE") == 0) {
				fingerprint_index_type = SPARSE_INDEX;
			} else {
				puts("unknown index type");
				puts("type -h or --help for help.");
				return 0;
			}
			break;
		case 'c':
			if (strcmp(optarg, "LRU") == 0) {
				read_cache_type = LRU_CACHE;
			} else if (strcmp(optarg, "OPT") == 0) {
				read_cache_type = OPT_CACHE;
			} else if (strcmp(optarg, "ASM") == 0) {
				read_cache_type = ASM_CACHE;
			} else {
				printf("unknown cache type");
				puts("type -h or --help for help.");
				return 0;
			}
			break;
		case 'C':
			read_cache_size = atoi(optarg);
			break;
		case 'R':
			if (strcmp(optarg, "NO") == 0) {
				rewriting_algorithm = NO_REWRITING;
			} else if (strcmp(optarg, "CFL") == 0) {
				rewriting_algorithm = CFL_REWRITING;
			} else if (strcmp(optarg, "CBR") == 0) {
				rewriting_algorithm = CBR_REWRITING;
			} else if (strcmp(optarg, "CAP") == 0) {
				rewriting_algorithm = CAP_REWRITING;
			} else if (strcmp(optarg, "ECAP") == 0) {
				rewriting_algorithm = ECAP_REWRITING;
			} else if (strcmp(optarg, "HBR") == 0) {
				enable_hbr = TRUE;
			} else {
				puts("unknown rewriting algorithm\n");
				puts("type -h or --help for help.");
				return 0;
			}
			break;
		case 'T':
			capping_T = atoi(optarg);
			break;
		case 'a':
			capping_segment_size = atoi(optarg) * 1024 * 1024;
			break;
		case 'p':
			cfl_usage_threshold = atoi(optarg) / 100.0;
			break;
		case 'u':
			hbr_usage_threshold = atof(optarg);
			break;
		case 'm':
			minimal_rewrite_utility = atof(optarg);
			break;
		case 'l':
			rewrite_limit = atof(optarg);
			break;
		case 'e':
			enable_hbr = TRUE;
			break;
		case 'w':
			optimal_cache_window_size = atoi(optarg);
			break;
		case 'h':
			job_type = HELP_JOB;
			break;
		case 'S':
			stream_context_size = atoi(optarg) * 1024 * 1024;
			break;
		case 'E':
			enable_cache_filter = TRUE;
			break;
		case 'I':
			if (strcmp(optarg, "NO") == 0) {
				simulation_level = SIMULATION_NO;
			} else if (strcmp(optarg, "RECOVERY") == 0) {
				simulation_level = SIMULATION_RECOVERY;
			} else if (strcmp(optarg, "APPEND") == 0) {
				simulation_level = SIMULATION_APPEND;
			} else if (strcmp(optarg, "ALL") == 0) {
				simulation_level = SIMULATION_ALL;
			} else {
				dprint("An wrong simulation_level!");
				return 0;
			}
			break;
		default:
			return 0;
		}
	}

	if (init_destor_stat() == FAILURE) {
		puts("Failed to init server stat!");
		return 0;
	}
	switch (job_type) {
	case BACKUP_JOB:
		if (argc > optind) {
			strcpy(path, argv[optind]);
		} else {
			puts("backup job needs a protected path!");
			puts("type -h or --help for help.");
			return 0;
		}
		init_jobmanage();
		init_container_volume();
		backup_server(path);
		destroy_container_volume();
		break;
	case RESTORE_JOB:
		if (revision < 0) {
			puts("required a job id for restore job!");
			puts("type -h or --help for help.");
			return 0;
		}
		if (argc > optind) {
			strcpy(path, argv[optind]);
		} else {
			puts("restore job needs a target directory!");
			puts("type -h or --help for help.");
			return 0;
		}
		init_jobmanage();
		init_container_volume();
		restore_server(revision, path[0] == 0 ? 0 : path);
		destroy_container_volume();
		break;
	case STAT_JOB:
		print_destor_stat();
		break;
	case HELP_JOB:
		print_help();
		break;
	case MAKE_TRACE: {
		if (argc > optind) {
			strcpy(path, argv[optind]);
		} else {
			puts("Making a trance needs an input raw file!");
			puts("type -h or --help for help.");
			return 0;
		}

		make_trace(path);
		break;
	}
	default:
		puts("invalid job type!");
		puts("type -h or --help for help.");
		return 0;
	}
	free_destor_stat();

	return 0;
}

