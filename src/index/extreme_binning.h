#ifndef EXTREME_BINNING_H_
#define EXTREME_BINNING_H_

#include "../global.h"

typedef struct bin_volume_tag BinVolume;
typedef struct bin_tag Bin;
typedef struct primary_item_tag PrimaryItem;

struct bin_volume_tag {
	int32_t current_bin_num;
	int64_t current_volume_length;
	char filename[256];
	int64_t level;
};

struct bin_tag {
	Fingerprint representative_fingerprint;
	/* fingerprint-container_id map */
	GHashTable *fingers;
	int64_t address;
	BOOL dirty;
	int32_t reference;
};

struct primary_item_tag {
	Fingerprint representative_fingerprint;
	int64_t bin_addr;
};

BOOL extreme_binning_init();
void extreme_binning_destroy();
ContainerId extreme_binning_search(Fingerprint *fingerprint,
		EigenValue *eigenvalue);
void extreme_binning_update(Fingerprint *finger, ContainerId container_id,
		EigenValue *eigenvalue, BOOL update);
#endif
