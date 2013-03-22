/* chunking.h
the main fuction is to chunking the file!
*/

//#define INT64(n) n##LL
#define MSB64 0x8000000000000000LL
#define MAXBUF (128*1024) 


#define FINGERPRINT_PT  0xbfe6b8a5bf378d83LL
#define BREAKMARK_VALUE 0x78
#define MIN_CHUNK_SIZE  2048
#define MAX_CHUNK_SIZE  65536

//UINT64 polymmult(UINT64 x, UINT64 y, UINT64 d);

//UINT64 polymod(UINT64 nh, UINT64 nl, UINT64 d);

//UINT64 append8(UINT64 p, UCHAR m);


//UINT64 polymmult(UINT64 x, UINT64 y, UINT64 d);

//void Rabin_init(UINT64 poly);

//void Rabin_reset();


int chunk_data(unsigned char *p, int n);
void chunkAlg_init();
void windows_reset();
