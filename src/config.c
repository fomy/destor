/*
 * destor_dedup.c
 *  
 *  Created on: Dec 9, 2012
 *      Author: fumin
 */

#include "destor.h"

int yesnotoi(char *s) {
	if (strcasecmp(s, "yes") == 0)
		return 1;
	else if (strcasecmp(s, "no") == 0)
		return 0;
	else
		return -1;
}

void load_config_from_string(sds config) {
	char *err = NULL;
	int linenum = 0, totlines, i;
	sds *lines = sdssplitlen(config, strlen(config), "\n", 1, &totlines);

	for (i = 0; i < totlines; i++) {
		sds *argv;
		int argc;

		linenum = i + 1;
		lines[i] = sdstrim(lines[i], " \t\r\n");

		if (lines[i][0] == '#' || lines[i][0] == '\0')
			continue;

		argv = sdssplitargs(lines[i], &argc);
		if (argv == NULL) {
			err = "Unbalanced quotes in configuration line";
			goto loaderr;
		}

		if (argc == 0) {
			sdsfreesplitres(argv, argc);
			continue;
		}
		sdstolower(argv[0]);

		if (strcasecmp(argv[0], "working-directory") == 0 && argc == 2) {
			destor.working_directory = sdscpy(destor.working_directory,
					argv[1]);
			destor.working_directory = sdscat(destor.working_directory, "/");
		} else if (strcasecmp(argv[0], "simulation-level") == 0 && argc == 2) {
			if (strcasecmp(argv[1], "all") == 0) {
				destor.simulation_level = SIMULATION_ALL;
			} else if (strcasecmp(argv[1], "append") == 0) {
				destor.simulation_level = SIMULATION_APPEND;
			} else if (strcasecmp(argv[1], "restore") == 0) {
				destor.simulation_level = SIMULATION_RESTORE;
			} else if (strcasecmp(argv[1], "no") == 0) {
				destor.simulation_level = SIMULATION_NO;
			} else {
				err = "Invalid simulation level";
				goto loaderr;
			}
		} else if (strcasecmp(argv[0], "trace-format") == 0 && argc == 2) {
            if (strcasecmp(argv[1], "destor") == 0) {
				destor.trace_format = TRACE_DESTOR;
			} else if (strcasecmp(argv[1], "fsl") == 0) {
				destor.trace_format = TRACE_FSL;
			} else {
				err = "Invalid trace format";
				goto loaderr;
			}
		} else if (strcasecmp(argv[0], "log-level") == 0 && argc == 2) {
			if (strcasecmp(argv[1], "debug") == 0) {
				destor.verbosity = DESTOR_DEBUG;
			} else if (strcasecmp(argv[1], "verbose") == 0) {
				destor.verbosity = DESTOR_VERBOSE;
			} else if (strcasecmp(argv[1], "notice") == 0) {
				destor.verbosity = DESTOR_NOTICE;
			} else if (strcasecmp(argv[1], "warning") == 0) {
				destor.verbosity = DESTOR_WARNING;
			} else {
				err = "Invalid log level";
				goto loaderr;
			}
		} else if (strcasecmp(argv[0], "chunk-algorithm") == 0 && argc == 2) {
			if (strcasecmp(argv[1], "fixed") == 0) {
				destor.chunk_algorithm = CHUNK_FIXED;
			} else if (strcasecmp(argv[1], "rabin") == 0) {
				destor.chunk_algorithm = CHUNK_RABIN;
			} else if (strcasecmp(argv[1], "normalized rabin") == 0) {
				destor.chunk_algorithm = CHUNK_NORMALIZED_RABIN;
			} else if (strcasecmp(argv[1], "tttd") == 0) {
				destor.chunk_algorithm = CHUNK_TTTD;
			} else if (strcasecmp(argv[1], "file") == 0) {
				destor.chunk_algorithm = CHUNK_FILE;
			} else if (strcasecmp(argv[1], "ae") == 0) {
				destor.chunk_algorithm = CHUNK_AE;
			} else {
				err = "Invalid chunk algorithm";
				goto loaderr;
			}
		} else if (strcasecmp(argv[0], "chunk-avg-size") == 0 && argc == 2) {
			destor.chunk_avg_size = atoi(argv[1]);
		} else if (strcasecmp(argv[0], "chunk-max-size") == 0 && argc == 2) {
			destor.chunk_max_size = atoi(argv[1]);
		} else if (strcasecmp(argv[0], "chunk-min-size") == 0 && argc == 2) {
			destor.chunk_min_size = atoi(argv[1]);
		} else if (strcasecmp(argv[0], "fingerprint-index") == 0 && argc >= 3) {
			if (strcasecmp(argv[1], "exact") == 0) {
				destor.index_category[0] = INDEX_CATEGORY_EXACT;
			} else if (strcasecmp(argv[1], "near-exact") == 0) {
				destor.index_category[0] = INDEX_CATEGORY_NEAR_EXACT;
			} else {
				err = "Invalid index category";
				goto loaderr;
			}

			if (strcasecmp(argv[2], "physical") == 0) {
				destor.index_category[1] = INDEX_CATEGORY_PHYSICAL_LOCALITY;
			} else if (strcasecmp(argv[2], "logical") == 0) {
				destor.index_category[1] = INDEX_CATEGORY_LOGICAL_LOCALITY;
			} else {
				err = "Invalid index category";
				goto loaderr;
			}

			if (argc > 3) {
				if (strcasecmp(argv[3], "ddfs") == 0) {
					assert(destor.index_category[0] == INDEX_CATEGORY_EXACT 
                            && destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY);
					destor.index_specific = INDEX_SPECIFIC_DDFS;
				} else if (strcasecmp(argv[3], "sampled index") == 0) {
					assert(destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT 
                            && destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY);
					destor.index_specific = INDEX_SPECIFIC_SAMPLED;
				} else if (strcasecmp(argv[3], "block locality caching") == 0) {
					assert(destor.index_category[0] == INDEX_CATEGORY_EXACT 
                            && destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY);
					destor.index_specific =	INDEX_SPECIFIC_BLOCK_LOCALITY_CACHING;
				} else if (strcasecmp(argv[3], "extreme binning") == 0) {
					assert(destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT 
                            && destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY);
					destor.index_specific = INDEX_SPECIFIC_EXTREME_BINNING;
				} else if (strcasecmp(argv[3], "sparse index") == 0) {
					assert(destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT 
                            && destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY);
					destor.index_specific = INDEX_SPECIFIC_SPARSE;
				} else if (strcasecmp(argv[3], "silo") == 0) {
					assert(destor.index_category[0] == INDEX_CATEGORY_NEAR_EXACT 
                            && destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY);
					destor.index_specific = INDEX_SPECIFIC_SILO;
				} else {
					err = "Invalid index specific";
					goto loaderr;
				}
			}
		} else if (strcasecmp(argv[0], "fingerprint-index-cache-size")
				== 0 && argc == 2) {
			destor.index_cache_size = atoi(argv[1]);
		} else if (strcasecmp(argv[0], "fingerprint-index-key-value") == 0
				&& argc == 2) {
			if (strcasecmp(argv[1], "htable") == 0) {
				destor.index_key_value_store = INDEX_KEY_VALUE_HTABLE;
			} else {
				err = "Invalid key-value store";
				goto loaderr;
			}
		} else if (strcasecmp(argv[0], "fingerprint-index-key-size") == 0
				&& argc == 2) {
			destor.index_key_size = atoi(argv[1]);
		} else if (strcasecmp(argv[0], "fingerprint-index-value-length") == 0
				&& argc == 2) {
			destor.index_value_length = atoi(argv[1]);
		} else if (strcasecmp(argv[0], "fingerprint-index-bloom-filter") == 0
				&& argc == 2) {
			destor.index_bloom_filter_size = atoi(argv[1]);
		} else if (strcasecmp(argv[0], "fingerprint-index-sampling-method") == 0
				&& argc >= 2) {
			if (strcasecmp(argv[1], "optmin") == 0)
				destor.index_sampling_method[0] = INDEX_SAMPLING_OPTIMIZED_MIN;
			else if (strcasecmp(argv[1], "random") == 0)
				destor.index_sampling_method[0] = INDEX_SAMPLING_RANDOM;
			else if (strcasecmp(argv[1], "min") == 0)
				destor.index_sampling_method[0] = INDEX_SAMPLING_MIN;
			else if (strcasecmp(argv[1], "uniform") == 0)
				destor.index_sampling_method[0] = INDEX_SAMPLING_UNIFORM;
			else {
				err = "Invalid feature method!";
				goto loaderr;
			}

			if (argc > 2) {
				destor.index_sampling_method[1] = atoi(argv[2]);
			} else {
				destor.index_sampling_method[1] = 0;
			}
		} else if (strcasecmp(argv[0], "fingerprint-index-segment-algorithm")
				== 0 && argc >= 2) {
			if (strcasecmp(argv[1], "fixed") == 0)
				destor.index_segment_algorithm[0] = INDEX_SEGMENT_FIXED;
			else if (strcasecmp(argv[1], "file-defined") == 0)
				destor.index_segment_algorithm[0] = INDEX_SEGMENT_FILE_DEFINED;
			else if (strcasecmp(argv[1], "content-defined") == 0)
				destor.index_segment_algorithm[0] =	INDEX_SEGMENT_CONTENT_DEFINED;
			else {
				err = "Invalid segment algorithm";
				goto loaderr;
			}

			if (argc > 2) {
				assert(destor.index_segment_algorithm[0] != INDEX_SEGMENT_FILE_DEFINED);
				destor.index_segment_algorithm[1] = atoi(argv[2]);
			}
		} else if (strcasecmp(argv[0], "fingerprint-index-segment-boundary") == 0
				&& argc == 3) {
			destor.index_segment_min = atoi(argv[1]);
			destor.index_segment_max = atoi(argv[2]);
		} else if (strcasecmp(argv[0], "fingerprint-index-segment-selection")
				== 0 && argc >= 2) {
			destor.index_segment_selection_method[1] = 1;
			if (strcasecmp(argv[1], "base") == 0)
				destor.index_segment_selection_method[0] = INDEX_SEGMENT_SELECT_BASE;
			else if (strcasecmp(argv[1], "top") == 0) {
				destor.index_segment_selection_method[0] = INDEX_SEGMENT_SELECT_TOP;
				if (argc > 2)
					destor.index_segment_selection_method[1] = atoi(argv[2]);
			} else if (strcasecmp(argv[1], "mix") == 0)
				destor.index_segment_selection_method[0] = INDEX_SEGMENT_SELECT_MIX;
			else {
				err = "Invalid selection method!";
				goto loaderr;
			}
		} else if (strcasecmp(argv[0], "fingerprint-index-segment-prefetching")	== 0 && argc == 2) {
			destor.index_segment_prefech = atoi(argv[1]);
		} else if (strcasecmp(argv[0], "rewrite-algorithm") == 0 && argc >= 2) {
			if (strcasecmp(argv[1], "no") == 0)
				destor.rewrite_algorithm[0] = REWRITE_NO;
			else if (strcasecmp(argv[1], "cfl-based selective deduplication")
					== 0 || strcasecmp(argv[1], "cfl") == 0)
				destor.rewrite_algorithm[0] =
				REWRITE_CFL_SELECTIVE_DEDUPLICATION;
			else if (strcasecmp(argv[1], "context-based rewriting") == 0
					|| strcasecmp(argv[1], "cbr") == 0)
				destor.rewrite_algorithm[0] = REWRITE_CONTEXT_BASED;
			else if (strcasecmp(argv[1], "capping") == 0
					|| strcasecmp(argv[1], "cap") == 0)
				destor.rewrite_algorithm[0] = REWRITE_CAPPING;
			else {
				err = "Invalid rewriting algorithm";
				goto loaderr;
			}

			if (argc > 2) {
				assert(destor.rewrite_algorithm != REWRITE_NO);
				destor.rewrite_algorithm[1] = atoi(argv[2]);
			} else {
				destor.rewrite_algorithm[1] = 1024;
			}
		} else if (strcasecmp(argv[0], "rewrite-enable-cfl-switch") == 0
				&& argc == 2) {
			destor.rewrite_enable_cfl_switch = yesnotoi(argv[1]);
		} else if (strcasecmp(argv[0], "rewrite-cfl-require") == 0
				&& argc == 2) {
			destor.rewrite_cfl_require = atof(argv[1]);
		} else if (strcasecmp(argv[0], "rewrite-cfl-usage-threshold") == 0
				&& argc == 2) {
			destor.rewrite_cfl_usage_threshold = atof(argv[1]);
		} else if (strcasecmp(argv[0], "rewrite-cbr-limit") == 0 && argc == 2) {
			destor.rewrite_cbr_limit = atof(argv[1]);
		} else if (strcasecmp(argv[0], "rewrite-cbr-minimal-utility") == 0
				&& argc == 2) {
			destor.rewrite_cbr_minimal_utility = atof(argv[1]);
		} else if (strcasecmp(argv[0], "rewrite-capping-level") == 0
				&& argc == 2) {
			destor.rewrite_capping_level = atoi(argv[1]);
		} else if (strcasecmp(argv[0], "rewrite-enable-har") == 0
				&& argc == 2) {
			destor.rewrite_enable_har = yesnotoi(argv[1]);
		} else if (strcasecmp(argv[0], "rewrite-har-utilization-threshold") == 0
				&& argc == 2) {
			destor.rewrite_har_utilization_threshold = atof(argv[1]);
		} else if (strcasecmp(argv[0], "rewrite-har-rewrite-limit") == 0
				&& argc == 2) {
			destor.rewrite_har_rewrite_limit = atof(argv[1]);
		} else if (strcasecmp(argv[0], "rewrite-enable-cache-aware") == 0
				&& argc == 2) {
			destor.rewrite_enable_cache_aware = yesnotoi(argv[1]);
		} else if (strcasecmp(argv[0], "restore-cache") == 0 && argc == 3) {
			if (strcasecmp(argv[1], "lru") == 0)
				destor.restore_cache[0] = RESTORE_CACHE_LRU;
			else if (strcasecmp(argv[1], "optimal cache") == 0
					|| strcasecmp(argv[1], "opt") == 0)
				destor.restore_cache[0] = RESTORE_CACHE_OPT;
			else if (strcasecmp(argv[1], "forward assembly") == 0
					|| strcasecmp(argv[1], "asm") == 0)
				destor.restore_cache[0] = RESTORE_CACHE_ASM;
			else {
				err = "Invalid restore cache";
				goto loaderr;
			}

			destor.restore_cache[1] = atoi(argv[2]);
		} else if (strcasecmp(argv[0], "restore-opt-window-size") == 0
				&& argc == 2) {
			destor.restore_opt_window_size = atoi(argv[1]);
		} else if (strcasecmp(argv[0], "backup-retention-time") == 0
				&& argc == 2) {
			destor.backup_retention_time = atoi(argv[1]);
		} else {
			err = "Bad directive or wrong number of arguments";
			goto loaderr;
		}
		sdsfreesplitres(argv, argc);
	}
	sdsfreesplitres(lines, totlines);
	return;

	loaderr: fprintf(stderr, "\n*** FATAL CONFIG FILE ERROR in destor ***\n");
	fprintf(stderr, "Reading the configuration file, at line %d\n", linenum);
	fprintf(stderr, ">>> '%s'\n", lines[i]);
	fprintf(stderr, "%s\n", err);
	exit(1);
}

void load_config() {
	sds config = sdsempty();
	char buf[DESTOR_CONFIGLINE_MAX + 1];

	FILE *fp;
	if ((fp = fopen("destor.config", "r")) == 0) {
		destor_log(DESTOR_WARNING, "No destor.config file!");
		return;
	}

	while (fgets(buf, DESTOR_CONFIGLINE_MAX + 1, fp) != NULL)
		config = sdscat(config, buf);

	fclose(fp);
	load_config_from_string(config);
	sdsfree(config);
}
