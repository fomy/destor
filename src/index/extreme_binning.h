#ifndef EXTREME_BINNING_H_
#define EXTREME_BINNING_H_

#include "../global.h"

BOOL extreme_binning_init();
void extreme_binning_destroy();
ContainerId extreme_binning_search(Fingerprint *fingerprint,
		EigenValue *eigenvalue);
void extreme_binning_update(Fingerprint *finger, ContainerId container_id,
		EigenValue *eigenvalue, BOOL update);
#endif
