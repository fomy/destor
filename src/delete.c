/*
 * delete_server.c
 *
 *  Created on: Jun 21, 2012
 *      Author: fumin
 */
#include "global.h"

int delete_server(int sock, int jobid) {

	puts("delete");
	if (dnet_send(sock, "SUCCESS", 8)) {
		printf("Failed to response delete!\n");
		return FAILURE;
	}
	return SUCCESS;
}

