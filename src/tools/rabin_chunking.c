#include <stdint.h>/*uint32_t*/
#include <stdlib.h> /*size_t*/
#include <string.h>/*memset*/
#include "rabin_chunking.h"
#include "../global.h"

extern int chunking_algorithm;

#define SLIDE(m,fingerprint,bufPos,buf) do{	\
	    unsigned char om;   \
	    u_int64_t x;	 \
		if (++bufPos >= size)  \
        bufPos = 0;				\
        om = buf[bufPos];		\
        buf[bufPos] = m;		 \
		fingerprint ^= U[om];	 \
		x = fingerprint >> shift;  \
		fingerprint <<= 8;		   \
		fingerprint |= m;		  \
		fingerprint ^= T[x];	 \
}while(0)

typedef unsigned int UINT32;
typedef unsigned long long int UINT64;
//time_t backup_now;

enum {
	size = 48
};
UINT64 fingerprint;
int bufpos;
UINT64 U[256];
unsigned char buf[size];
int shift;
UINT64 T[256];
UINT64 poly;

char *eFiles[] = { ".pdf", ".rmv", "ra", ".bmp", ".vmem", ".vmdk", ".jpeg",
		".rmvb", ".exe", ".mtv", "\\Program Files", "C:\\" };

size_t _last_pos;
size_t _cur_pos;

unsigned int _num_chunks;

extern uint32_t chunk_size;
extern uint32_t max_chunk_size;
extern uint32_t min_chunk_size;

const char bytemsb[0x100] = { 0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5,
		5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6,
		6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, };

/***********************************************the rabin**********************************************/
static uint32_t fls32(UINT32 v) {
	if (v & 0xffff0000) {
		if (v & 0xff000000)
			return 24 + bytemsb[v >> 24];
		else
			return 16 + bytemsb[v >> 16];
	}
	if (v & 0x0000ff00)
		return 8 + bytemsb[v >> 8];
	else
		return bytemsb[v];
}

static uint32_t fls64(UINT64 v) {
	UINT32 h;
	if ((h = v >> 32))
		return 32 + fls32(h);
	else
		return fls32((UINT32) v);
}

UINT64 polymod(UINT64 nh, UINT64 nl, UINT64 d) {

	int k = fls64(d) - 1;
	int i;

	//printf ("polymod : k = %d\n", k);

	d <<= 63 - k;

	//printf ("polymod : d = %llu\n", d);
	//printf ("polymod : MSB64 = %llu\n", MSB64);

	if (nh) {
		if (nh & MSB64)
			nh ^= d;

		//printf ("polymod : nh = %llu\n", nh);

		for (i = 62; i >= 0; i--)
			if (nh & ((UINT64) 1) << i) {
				nh ^= d >> (63 - i);
				nl ^= d << (i + 1);

				//printf ("polymod : i = %d\n", i);
				//printf ("polymod : shift1 = %llu\n", (d >> (63 - i)));
				//printf ("polymod : shift2 = %llu\n", (d << (i + 1)));
				//printf ("polymod : nh = %llu\n", nh);
				//printf ("polymod : nl = %llu\n", nl);

			}
	}
	for (i = 63; i >= k; i--) {
		if (nl & (long long int) (1) << i)
			nl ^= d >> 63 - i;

		//printf ("polymod : nl = %llu\n", nl);

	}

	//printf ("polymod : returning %llu\n", nl);

	return nl;
}

void polymult(UINT64 *php, UINT64 *plp, UINT64 x, UINT64 y) {

	int i;

	//printf ("polymult (x %llu y %llu)\n", x, y);

	UINT64 ph = 0, pl = 0;
	if (x & 1)
		pl = y;
	for (i = 1; i < 64; i++)
		if (x & ((long long int) (1) << i)) {

			//printf ("polymult : i = %d\n", i);
			//printf ("polymult : ph = %llu\n", ph);
			//printf ("polymult : pl = %llu\n", pl);
			//printf ("polymult : y = %llu\n", y);
			//printf ("polymult : ph ^ y >> (64-i) = %llu\n", (ph ^ y >> (64-i)));
			//printf ("polymult : pl ^ y << i = %llu\n", (pl ^ y << i));

			ph ^= y >> (64 - i);
			pl ^= y << i;

			//printf ("polymult : ph %llu pl %llu\n", ph, pl);

		}
	if (php)
		*php = ph;
	if (plp)
		*plp = pl;

	//printf ("polymult : h %llu l %llu\n", ph, pl);

}

UINT64 append8(UINT64 p, unsigned char m) {
	return ((p << 8) | m) ^ T[p >> shift];
}

UINT64 slide8(unsigned char m) {
	unsigned char om;
	//printf("this char is %c\n",m);
	if (++bufpos >= size)
		bufpos = 0;
	om = buf[bufpos];
	buf[bufpos] = m;
	return fingerprint = append8(fingerprint ^ U[om], m);
}

UINT64 polymmult(UINT64 x, UINT64 y, UINT64 d) {

	//printf ("polymmult (x %llu y %llu d %llu)\n", x, y, d);

	UINT64 h, l;
	polymult(&h, &l, x, y);
	return polymod(h, l, d);
}

void calcT(UINT64 poly) {

	int j;
	UINT64 T1;

	//printf ("rabinpoly::calcT ()\n");

	int xshift = fls64(poly) - 1;
	shift = xshift - 8;
	T1 = polymod(0, (long long int) (1) << xshift, poly);
	for (j = 0; j < 256; j++) {
		T[j] = polymmult(j, T1, poly) | ((UINT64) j << xshift);

		//printf ("rabinpoly::calcT tmp = %llu\n", polymmult (j, T1, poly));
		//printf ("rabinpoly::calcT shift = %llu\n", ((UINT64) j << xshift));
		//printf ("rabinpoly::calcT xshift = %d\n", xshift);
		//printf ("rabinpoly::calcT T[%d] = %llu\n", j, T[j]);

	}

	//printf ("rabinpoly::calcT xshift = %d\n", xshift);
	//printf ("rabinpoly::calcT T1 = %llu\n", T1);
	//printf ("rabinpoly::calcT T = {");
	//for (i=0; i< 256; i++)
	//printf ("\t%llu \n", T[i]);
	//printf ("}\n");

}

void rabinpoly_init(UINT64 p) {
	poly = p;
	calcT(poly);
}

void window_init(UINT64 poly) {

	int i;
	UINT64 sizeshift;

	rabinpoly_init(poly);
	fingerprint = 0;
	bufpos = -1;
	sizeshift = 1;
	for (i = 1; i < size; i++)
		sizeshift = append8(sizeshift, 0);
	for (i = 0; i < 256; i++)
		U[i] = polymmult(i, sizeshift, poly);
	memset((char*) buf, 0, sizeof(buf));
}

void windows_reset() {
	fingerprint = 0;
	//memset((char*) buf,0,sizeof (buf));
	//memset((char*) chunk,0,sizeof (chunk));
}

void chunkAlg_init() {
	window_init(FINGERPRINT_PT);
	_last_pos = 0;
	_cur_pos = 0;
	windows_reset();
	_num_chunks = 0;
}

int rabin_chunk_data(unsigned char *p, int n) {

	UINT64 f_break = 0;
	UINT64 count = 0;
	UINT64 fingerprint = 0;
	int i = 1, bufPos = -1;

	unsigned char om;
	u_int64_t x;

	unsigned char buf[128];
	memset((char*) buf, 0, 128);

	if (n <= min_chunk_size)
		return n;
	else
		i = min_chunk_size;
	while (i <= n) {

		SLIDE(p[i - 1], fingerprint, bufPos, buf);
		if (chunking_algorithm == RABIN_CHUNK) {
			if (((fingerprint & (chunk_size - 1)) == BREAKMARK_VALUE
					&& i >= min_chunk_size) || i >= max_chunk_size || i == n) {
				break;
			} else
				i++;
		} else if (chunking_algorithm == NRABIN_CHUNK) {
			if (i < chunk_size) {
				if (((fingerprint & (chunk_size * 2 - 1)) == BREAKMARK_VALUE
						&& i >= min_chunk_size) || i >= max_chunk_size
						|| i == n) {
					break;
				} else
					i++;
			} else {
				if (((fingerprint & (chunk_size / 2 - 1)) == BREAKMARK_VALUE
						&& i >= min_chunk_size) || i >= max_chunk_size
						|| i == n) {
					break;
				} else
					i++;
			}

		}
	}
	return i;
}
