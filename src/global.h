/*
 * destor.h
 *
 *  Created on: May 21, 2012
 *      Author: fumin
 */

#ifndef SERVER_GLOBAL_H_
#define SERVER_GLOBAL_H_
#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/types.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>

#include <sys/socket.h>
#include <arpa/inet.h>

#include <openssl/sha.h>
#include <glib.h>
#include <getopt.h>

#include "tools/serial.h"

//#define _LARGEFILE_SOURCE
//#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64

//#define NULL 0
#define SUCCESS 0
#define FAILURE -1

typedef int BOOL;
//#define FALSE 0
//#define TRUE !(FALSE)

#define STREAM_END -2 /* indicates the end of stream */
#define FILE_END -1 /* indicates the end of file */

#define TIMER_DECLARE(b,e) struct timeval b,e;
#define TIMER_BEGIN(b) gettimeofday(&b, NULL);
#define TIMER_END(t,b,e) gettimeofday(&e, NULL); \
    (t)+=e.tv_usec-b.tv_usec+1000000*(e.tv_sec-b.tv_sec);

#define dprint(s) {printf("%s, %d:",__FILE__,__LINE__);puts(s);}

/* filter type */
#define NO_REWRITING 1
#define CFL_REWRITING 2
#define CBR_REWRITING 3
//#define HBR_REWRITING 4
//#define HBR_CBR_REWRITING 5
//#define HBR_CFL_REWRITING 6
#define CAP_REWRITING 7
//#define HBR_CAP_REWRITING 8
#define ECAP_REWRITING 9

#define READ_BUFFER_SIZE (1024*1024)
typedef int32_t ContainerId; //container id
typedef unsigned char Fingerprint[20];

#define TMP_CONTAINER_ID (-1)

#define LRU_CACHE 1
#define OPT_CACHE 2
#define ASM_CACHE 3

#define RAM_INDEX 1
#define DDFS_INDEX 2
#define EXBIN_INDEX 3
#define SILO_INDEX 4
#define SPARSE_INDEX 5

#define FIXED_CHUNK 1
#define RABIN_CHUNK 2
/* Normalized rabin chunking */
#define NRABIN_CHUNK 3

/* Simuation-level, ascending */
#define SIMULATION_NO 0
#define SIMULATION_RECOVERY 1
#define SIMULATION_APPEND 2
#define SIMULATION_ALL 3
#endif /* SERVER_GLOBAL_H_ */
