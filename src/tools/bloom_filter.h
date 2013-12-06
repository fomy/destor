#ifndef BF_H_
#define BF_H_

/* config options */
/* 2^FILTER_SIZE is the size of the filter in bits, i.e.,  
 * size 20 = 2^20 bits = 1 048 576 bits = 131 072 bytes = 128 KB */
#define FILTER_SIZE 30
#define NUM_HASHES 4
#define FILTER_SIZE_BYTES (1 << (FILTER_SIZE - 3))
#define FILTER_BITMASK ((1 << FILTER_SIZE) - 1)

void insert_word(unsigned char[], char *, int);
int in_dict(unsigned char[], char *, int);

#endif
