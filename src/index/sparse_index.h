#ifndef SPARSE_H_
#define SPARSE_H_

#include "../global.h"

typedef struct manifest{
    int64_t id;//first 6 bytes are address, last 2 bytes are length
    GSequence *matched_hooks;//Fingerprint sequence
    GHashTable *fingers;
}Manifest;

typedef struct hooks{
    int32_t size;
    Fingerprint hooks[0];
}Hooks;

#endif
