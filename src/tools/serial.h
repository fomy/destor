/*  Serialisation support functions from serial.c.  */
#include <stdint.h>
#include <assert.h>

extern void serial_int16(uint8_t * * const ptr, const int16_t v);
extern void serial_uint16(uint8_t * * const ptr, const uint16_t v);
extern void serial_int32(uint8_t * * const ptr, const int32_t v);
extern void serial_uint32(uint8_t * * const ptr, const uint32_t v);
extern void serial_int64(uint8_t * * ptr, int64_t v);
extern void serial_uint64(uint8_t * * const ptr, const uint64_t v);
//extern void serial_btime(uint8_t * * const ptr, const btime_t v);
//extern void serial_float64(uint8_t * * const ptr, const float64_t v);
extern void serial_string(uint8_t * * const ptr, const char * const str);

extern int16_t unserial_int16(uint8_t * * const ptr);
extern uint16_t unserial_uint16(uint8_t * * const ptr);
extern int32_t unserial_int32(uint8_t * * const ptr);
extern uint32_t unserial_uint32(uint8_t * * const ptr);
extern int64_t unserial_int64(uint8_t * * const ptr);
extern uint64_t unserial_uint64(uint8_t * * const ptr);
//extern btime_t unserial_btime(uint8_t * * const ptr);
//extern float64_t unserial_float64(uint8_t * * const ptr);
extern void unserial_string(uint8_t * * const ptr, char * const str);

/*

 Serialisation Macros

 These macros use a uint8_t pointer, ser_ptr, which must be
 defined by the code which uses them.

 */

#ifndef __SERIAL_H_
#define __SERIAL_H_ 1

/*  ser_declare  --  Declare ser_ptr locally within a function.  */
#define ser_declare     uint8_t *ser_ptr
#define unser_declare   uint8_t *ser_ptr

/*  ser_begin(x, s)  --  Begin serialisation into a buffer x of size s.  */
#define ser_begin(x, s) ser_ptr = ((uint8_t *)(x))
#define unser_begin(x, s) ser_ptr = ((uint8_t *)(x))

/*  ser_length  --  Determine length in bytes of serialised into a
 buffer x.  */
#define ser_length(x)  (ser_ptr - (uint8_t *)(x))
#define unser_length(x)  (ser_ptr - (uint8_t *)(x))

/*  ser_end(x, s)  --  End serialisation into a buffer x of size s.  */
#define ser_end(x, s)   assert(ser_length(x) <= (s))
#define unser_end(x, s)   assert(ser_length(x) <= (s))

/*  ser_check(x, s)  --  Verify length of serialised data in buffer x is
 expected length s.  */
#define ser_check(x, s) assert(ser_length(x) == (s))

/*                          Serialisation                   */

/*  8 bit signed integer  */
#define ser_int8(x)     *ser_ptr++ = (x)
/*  8 bit unsigned integer  */
#define ser_uint8(x)    *ser_ptr++ = (x)

/*  16 bit signed integer  */
#define ser_int16(x)    serial_int16(&ser_ptr, x)
/*  16 bit unsigned integer  */
#define ser_uint16(x)   serial_uint16(&ser_ptr, x)

/*  32 bit signed integer  */
#define ser_int32(x)    serial_int32(&ser_ptr, x)
/*  32 bit unsigned integer  */
#define ser_uint32(x)   serial_uint32(&ser_ptr, x)

/*  64 bit signed integer  */
#define ser_int64(x)    serial_int64(&ser_ptr, x)
/*  64 bit unsigned integer  */
#define ser_uint64(x)   serial_uint64(&ser_ptr, x)

/* btime -- 64 bit unsigned integer */
#define ser_btime(x)    serial_btime(&ser_ptr, x)

/*  64 bit IEEE floating point number  */
#define ser_float64(x)  serial_float64(&ser_ptr, x)

/*  Binary byte stream len bytes not requiring serialisation  */
#define ser_bytes(x, len) memcpy(ser_ptr, (x), (len)), ser_ptr += (len)

/*  Binary byte stream not requiring serialisation (length obtained by sizeof)  */
#define ser_struct(x)   ser_bytes(&(x), (sizeof (x)))

/* Binary string not requiring serialization */
#define ser_string(x)   serial_string(&ser_ptr, (x))

/*                         Unserialisation                  */

/*  8 bit signed integer  */
#define unser_int8(x)   (x) = *ser_ptr++
/*  8 bit unsigned integer  */
#define unser_uint8(x)  (x) = *ser_ptr++

/*  16 bit signed integer  */
#define unser_int16(x)  (x) = unserial_int16(&ser_ptr)
/*  16 bit unsigned integer  */
#define unser_uint16(x) (x) = unserial_uint16(&ser_ptr)

/*  32 bit signed integer  */
#define unser_int32(x)  (x) = unserial_int32(&ser_ptr)
/*  32 bit unsigned integer  */
#define unser_uint32(x) (x) = unserial_uint32(&ser_ptr)

/*  64 bit signed integer  */
#define unser_int64(x)  (x) = unserial_int64(&ser_ptr)
/*  64 bit unsigned integer  */
#define unser_uint64(x) (x) = unserial_uint64(&ser_ptr)

/* btime -- 64 bit unsigned integer */
#define unser_btime(x) (x) = unserial_btime(&ser_ptr)

/*  64 bit IEEE floating point number  */
#define unser_float64(x)(x) = unserial_float64(&ser_ptr)

/*  Binary byte stream len bytes not requiring serialisation  */
#define unser_bytes(x, len) memcpy((x), ser_ptr, (len)), ser_ptr += (len)

/*  Binary byte stream not requiring serialisation (length obtained by sizeof)  */
#define unser_struct(x)  unser_bytes(&(x), (sizeof (x)))

/* Binary string not requiring serialization */
#define unser_string(x) unserial_string(&ser_ptr, (x))

#endif /* __SERIAL_H_ */
