#ifndef RAMINDEX_H_
#define RAMINDEX_H_

int ram_index_init();
void ram_index_destroy();
ContainerId ram_index_search(Fingerprint *fp);
void ram_index_update(Fingerprint* finger, ContainerId id);

#endif
