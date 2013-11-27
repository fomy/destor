/*
 * restore.h
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */

#ifndef RESTORE_H_
#define RESTORE_H_

void send_restore_chunk(unsigned char *data, int32_t size);

void term_restore_queue();

#endif /* RESTORE_H_ */
