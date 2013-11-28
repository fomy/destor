/*
 * restore.h
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */

#ifndef RESTORE_H_
#define RESTORE_H_

void send_restore_chunk(struct chunk* c);

void term_restore_chunk_queue() ;

int recv_restore_recipe(void **cp);

#endif /* RESTORE_H_ */
