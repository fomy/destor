#include "index.h"
#include "ramindex.h"
#include "ddfsindex.h"
#include "extreme_binning.h"
#include "silo.h"
#include "sparse_index.h"

extern int fingerprint_index_type;

double search_time;
double update_time;

BOOL index_init() {
	search_time = 0;
	update_time = 0;
	switch (fingerprint_index_type) {
	case RAM_INDEX:
		puts("index=RAM");
		return ram_index_init();
	case DDFS_INDEX:
		puts("index=DDFS");
		return ddfs_index_init();
	case EXBIN_INDEX:
		puts("index=EXBIN");
		return extreme_binning_init();
	case SILO_INDEX:
		puts("index=SILO");
		return silo_init();
	case SPARSE_INDEX:
		puts("index=SPARSE_INDEX");
		return sparse_index_init();
	default:
		printf("%s, %d: Wrong index type!\n", __FILE__, __LINE__);
		return FALSE;
	}
}

void index_destroy() {
	switch (fingerprint_index_type) {
	case RAM_INDEX:
		ram_index_destroy();
		break;
	case DDFS_INDEX:
		ddfs_index_destroy();
		break;
	case EXBIN_INDEX:
		extreme_binning_destroy();
		break;
	case SILO_INDEX:
		silo_destroy();
		break;
	case SPARSE_INDEX:
		sparse_index_destroy();
		break;
	default:
		printf("%s, %d: Wrong index type!\n", __FILE__, __LINE__);
	}
}

/*
 * Call index_search() to obtain the container id of the chunk.
 * It should be called immediately after poping a chunk.
 */
ContainerId index_search(Fingerprint* fingerprint, EigenValue* eigenvalue) {
	TIMER_DECLARE(b, e);
	TIMER_BEGIN(b);
	ContainerId container_id;
	switch (fingerprint_index_type) {
	case RAM_INDEX:
		container_id = ram_index_search(fingerprint);
		break;
	case DDFS_INDEX:
		container_id = ddfs_index_search(fingerprint);
		break;
	case EXBIN_INDEX:
		container_id = extreme_binning_search(fingerprint, eigenvalue);
		break;
	case SILO_INDEX:
		container_id = silo_search(fingerprint, eigenvalue);
		break;
	case SPARSE_INDEX:
		container_id = sparse_index_search(fingerprint, eigenvalue);
		break;
	default:
		printf("%s, %d: Wrong index type!\n", __FILE__, __LINE__);
		return TMP_CONTAINER_ID;
	}
	TIMER_END(search_time, b, e);
	return container_id;
}

/*
 * Update index.
 * It should be called before pushing a chunk into fchunk_queue in filter phase.
 * The update parameter indicates whether the container_id is new.
 */
void index_update(Fingerprint* fingerprint, ContainerId container_id,
		EigenValue* eigenvalue, BOOL update) {
	/* The update determines wheter update except in SILO */
	TIMER_DECLARE(b, e);
	TIMER_BEGIN(b);
	switch (fingerprint_index_type) {
	case RAM_INDEX:
		if (update) {
			ram_index_update(fingerprint, container_id);
		}
		break;
	case DDFS_INDEX:
		if (update) {
			ddfs_index_update(fingerprint, container_id);
		}
		break;
	case EXBIN_INDEX:
		extreme_binning_update(fingerprint, container_id, eigenvalue, update);
		break;
	case SILO_INDEX:
		silo_update(fingerprint, container_id, eigenvalue, update);
		break;
	case SPARSE_INDEX:
		sparse_index_update(fingerprint, container_id, eigenvalue, update);
		break;
	default:
		printf("%s, %d: Wrong index type!\n", __FILE__, __LINE__);
	}
	TIMER_END(update_time, b, e);
}
