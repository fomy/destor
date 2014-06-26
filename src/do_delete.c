/*
 * delete_server.c
 *
 *  Created on: Jun 21, 2012
 *      Author: fumin
 */
#include "destor.h"
#include "storage/containerstore.h"
#include "index/index.h"
#include "cma.h"

/*
 * delete all jobs before jobid, including itself.
 * Find all containers in manifest whose time is earlier than jobid.
 * These containers can be reclaimed.
 * Read the metadata part of these containers,
 * and delete entries in fingerprint index.
 */
void do_delete(int jobid) {

	GHashTable *invalid_containers = trunc_manifest(jobid);

	init_index();

	close_index();
}
