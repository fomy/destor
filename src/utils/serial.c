/*
  *Serialisation Support Functions
  */
/*

        NOTE:  The following functions should work on any
               vaguely contemporary platform.  Production
               builds should use optimised macros (void
               on platforms with network byte order and IEEE
               floating point format as native.

*/
/*  serial_int16  --  Serialise a signed 16 bit integer.  */
#include "serial.h"
#include <string.h>
#include <arpa/inet.h>

void serial_int16(uint8_t * * const ptr, const int16_t v)
{
    int16_t vo = htons(v);

    memcpy(*ptr, &vo, sizeof vo);
    *ptr += sizeof vo;
}

/*  serial_uint16  --  Serialise an unsigned 16 bit integer.  */

void serial_uint16(uint8_t * * const ptr, const uint16_t v)
{
    uint16_t vo = htons(v);

    memcpy(*ptr, &vo, sizeof vo);
    *ptr += sizeof vo;
}

/*  serial_int32  --  Serialise a signed 32 bit integer.  */

void serial_int32(uint8_t * * const ptr, const int32_t v)
{
    int32_t vo = htonl(v);

    memcpy(*ptr, &vo, sizeof vo);
    *ptr += sizeof vo;
}

/*  serial_uint32  --  Serialise an unsigned 32 bit integer.  */

void serial_uint32(uint8_t * * const ptr, const uint32_t v)
{
    uint32_t vo = htonl(v);

    memcpy(*ptr, &vo, sizeof vo);
    *ptr += sizeof vo;
}

/*  serial_int64  --  Serialise a signed 64 bit integer.  */

void serial_int64(uint8_t * * const ptr, const int64_t v)
{
    if (htonl(1) == 1L) {
        memcpy(*ptr, &v, sizeof(int64_t));
    } else {
        int i;
        uint8_t rv[sizeof(int64_t)];
        uint8_t *pv = (uint8_t *) &v;

        for (i = 0; i < 8; i++) {
            rv[i] = pv[7 - i];
        }
        memcpy(*ptr, &rv, sizeof(int64_t));
    }
    *ptr += sizeof(int64_t);
}


/*  serial_uint64  --  Serialise an unsigned 64 bit integer.  */

void serial_uint64(uint8_t * * const ptr, const uint64_t v)
{
    if (htonl(1) == 1L) {
        memcpy(*ptr, &v, sizeof(uint64_t));
    } else {
        int i;
        uint8_t rv[sizeof(uint64_t)];
        uint8_t *pv = (uint8_t *) &v;

        for (i = 0; i < 8; i++) {
            rv[i] = pv[7 - i];
        }
        memcpy(*ptr, &rv, sizeof(uint64_t));
    }
    *ptr += sizeof(uint64_t);
}

void serial_string(uint8_t * * const ptr, const char * const str)
{
   int len = strlen(str) + 1;
   memcpy(*ptr, str, len);
   *ptr += len;
}


/*  unserial_int16  --  Unserialise a signed 16 bit integer.  */

int16_t unserial_int16(uint8_t * * const ptr)
{
    int16_t vo;

    memcpy(&vo, *ptr, sizeof vo);
    *ptr += sizeof vo;
    return ntohs(vo);
}

/*  unserial_uint16  --  Unserialise an unsigned 16 bit integer.  */

uint16_t unserial_uint16(uint8_t * * const ptr)
{
    uint16_t vo;

    memcpy(&vo, *ptr, sizeof vo);
    *ptr += sizeof vo;
    return ntohs(vo);
}

/*  unserial_int32  --  Unserialise a signed 32 bit integer.  */

int32_t unserial_int32(uint8_t * * const ptr)
{
    int32_t vo;

    memcpy(&vo, *ptr, sizeof vo);
    *ptr += sizeof vo;
    return ntohl(vo);
}

/*  unserial_uint32  --  Unserialise an unsigned 32 bit integer.  */

uint32_t unserial_uint32(uint8_t * * const ptr)
{
    uint32_t vo;

    memcpy(&vo, *ptr, sizeof vo);
    *ptr += sizeof vo;
    return ntohl(vo);
}

int64_t unserial_int64(uint8_t * * const ptr)
{
    int64_t v;

    if (htonl(1) == 1L) {
        memcpy(&v, *ptr, sizeof(int64_t));
    } else {
        int i;
        uint8_t rv[sizeof(int64_t)];
        uint8_t *pv = (uint8_t *) &v;

        memcpy(&v, *ptr, sizeof(int64_t));
        for (i = 0; i < 8; i++) {
            rv[i] = pv[7 - i];
        }
        memcpy(&v, &rv, sizeof(int64_t));
    }
    *ptr += sizeof(int64_t);
    return v;
}

/*  unserial_uint64  --  Unserialise an unsigned 64 bit integer.  */
uint64_t unserial_uint64(uint8_t * * const ptr)
{
    uint64_t v;

    if (htonl(1) == 1L) {
        memcpy(&v, *ptr, sizeof(uint64_t));
    } else {
        int i;
        uint8_t rv[sizeof(uint64_t)];
        uint8_t *pv = (uint8_t *) &v;

        memcpy(&v, *ptr, sizeof(uint64_t));
        for (i = 0; i < 8; i++) {
            rv[i] = pv[7 - i];
        }
        memcpy(&v, &rv, sizeof(uint64_t));
    }
    *ptr += sizeof(uint64_t);
    return v;
}

void unserial_string(uint8_t * * const ptr, char * const str)
{
   int len = strlen((char *) *ptr) + 1;
   memcpy(str, (char *) *ptr, len);
   *ptr += len;
}
