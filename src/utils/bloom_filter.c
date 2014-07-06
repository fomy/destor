/*******************************************************************************
*
* bloom_filter.c -- A simple bloom filter implementation
* by timdoug -- timdoug@gmail.com -- http://www.timdoug.com -- 2008-07-05
* see http://en.wikipedia.org/wiki/Bloom_filter
*
********************************************************************************
*
* Copyright (c) 2008, timdoug(@gmail.com) -- except for the hash functions
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*     * The name of the author may not be used to endorse or promote products
*       derived from this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY timdoug ``AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL timdoug BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*
********************************************************************************
*
* compile with (note -- ints should be >= 32 bits):
* gcc -Wall -Werror -g -ansi -pedantic -O2 bloom_filter.c -o bloom_filter
*
* example usage ("words" is from /usr/share/dict/words on debian):
* $ ./bloom_filter
* usage: ./bloom_filter dictionary word ...
* $ ./bloom_filter words test word words foo bar baz not_in_dict
* "test" in dictionary
* "word" in dictionary
* "words" in dictionary
* "foo" not in dictionary
* "bar" in dictionary
* "baz" not in dictionary
* "not_in_dict" not in dictionary
*
* this implementation uses 7 hash functions, the optimal number for an input
* of approx. 100,000 and a filter size of 2^20 bits -- a ~7.2x decrease in
* space needed (from 912KB to 128KB), with an false positive rate of 0.007.
* consult Wikipedia for optimal numbers for your input and desired size
*
* all the hash functions are under the Common Public License and from:
* http://www.partow.net/programming/hashfunctions/index.html
* although I haven't rigorously tested them -- for real use, you'll probably
* want to make sure they work well for your data set or choose others.
*
* other hash functions of interest:
* http://www.cse.yorku.ca/~oz/hash.html
* http://www.isthe.com/chongo/tech/comp/fnv/
* http://www.azillionmonkeys.com/qed/hash.html
* http://murmurhash.googlepages.com/
*
*******************************************************************************/

/***************************************************
 * This file has been modified by Min Fu in 11/21/2012.
 * get_hashes() has been replaced with an array of function pointers, hash_func[].
 * ************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "bloom_filter.h"


/* hash functions */
unsigned int RSHash  (unsigned char *, unsigned int);
unsigned int DJBHash (unsigned char *, unsigned int);
unsigned int FNVHash (unsigned char *, unsigned int);
unsigned int JSHash  (unsigned char *, unsigned int);
unsigned int PJWHash (unsigned char *, unsigned int);
unsigned int SDBMHash(unsigned char *, unsigned int);
unsigned int DEKHash (unsigned char *, unsigned int);

unsigned int (*hash_func[])(unsigned char *, unsigned int) = { 
    RSHash, 
    DJBHash, 
    FNVHash, 
    JSHash,
    PJWHash, 
    SDBMHash, 
    DEKHash
};

void insert_word(unsigned char filter[], char *str, int len)
{
	unsigned long hash[NUM_HASHES];
	int i;
	
	for (i = 0; i < NUM_HASHES; i++) {
        hash[i] = hash_func[i](str, len);
		/* xor-fold the hash into FILTER_SIZE bits */
		hash[i] = (hash[i] >> FILTER_SIZE) ^ 
		          (hash[i] & FILTER_BITMASK);
		/* set the bit in the filter */
		filter[hash[i] >> 3] |= 1 << (hash[i] & 7);
	}
}

int in_dict(unsigned char filter[], char *str, int len)
{
	unsigned int hash[NUM_HASHES];
	int i;
	
	for (i = 0; i < NUM_HASHES; i++) {
        hash[i] = hash_func[i](str, len);
		hash[i] = (hash[i] >> FILTER_SIZE) ^
		          (hash[i] & FILTER_BITMASK);
		if (!(filter[hash[i] >> 3] & (1 << (hash[i] & 7))))
			return 0;
	}
	
	return 1;
}

/****************\
| Hash Functions |
\****************/

unsigned int RSHash(unsigned char *str, unsigned int len)
{
   unsigned int b    = 378551;
   unsigned int a    = 63689;
   unsigned int hash = 0;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = hash * a + (*str);
      a    = a * b;
   }

   return hash;
}

unsigned int JSHash(unsigned char *str, unsigned int len)
{
   unsigned int hash = 1315423911;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash ^= ((hash << 5) + (*str) + (hash >> 2));
   }

   return hash;
}

unsigned int PJWHash(unsigned char *str, unsigned int len)
{
   const unsigned int BitsInUnsignedInt = (unsigned int)(sizeof(unsigned int) * 8);
   const unsigned int ThreeQuarters     = (unsigned int)((BitsInUnsignedInt  * 3) / 4);
   const unsigned int OneEighth         = (unsigned int)(BitsInUnsignedInt / 8);
   const unsigned int HighBits          = (unsigned int)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);
   unsigned int hash              = 0;
   unsigned int test              = 0;
   unsigned int i                 = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = (hash << OneEighth) + (*str);

      if((test = hash & HighBits)  != 0)
      {
         hash = (( hash ^ (test >> ThreeQuarters)) & (~HighBits));
      }
   }

   return hash;
}

unsigned int SDBMHash(unsigned char *str, unsigned int len)
{
   unsigned int hash = 0;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = (*str) + (hash << 6) + (hash << 16) - hash;
   }

   return hash;
}

unsigned int DJBHash(unsigned char *str, unsigned int len)
{
   unsigned int hash = 5381;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = ((hash << 5) + hash) + (*str);
   }

   return hash;
}

unsigned int DEKHash(unsigned char *str, unsigned int len)
{
   unsigned int hash = len;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = ((hash << 5) ^ (hash >> 27)) ^ (*str);
   }
   return hash;
}

unsigned int FNVHash(unsigned char *str, unsigned int len)
{
   const unsigned int fnv_prime = 0x811C9DC5;
   unsigned int hash      = 0;
   unsigned int i         = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash *= fnv_prime;
      hash ^= (*str);
   }

   return hash;
}
