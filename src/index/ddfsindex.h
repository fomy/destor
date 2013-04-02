#ifndef DDFSINDEX_H_
#define DDFSINDEX_H_

int ddfs_index_init();
void ddfs_index_destroy();
ContainerId ddfs_index_search(Fingerprint *fp);
void ddfs_index_update(Fingerprint* finger, ContainerId id);

#endif
