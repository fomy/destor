/* chunking.h
 the main fuction is to chunking the file!
 */

//#define INT64(n) n##LL
#define MSB64 0x8000000000000000LL
#define MAXBUF (128*1024) 

#define FINGERPRINT_PT  0xbfe6b8a5bf378d83LL
#define BREAKMARK_VALUE 0x78

int rabin_chunk_data(unsigned char *p, int n);
void chunkAlg_init();
void windows_reset();
