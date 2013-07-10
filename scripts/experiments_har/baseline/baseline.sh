#!/bin/sh

./gcc_base.sh RAM
#./gcc_base.sh DDFS 
#./gcc_base.sh EXBIN 
#./gcc_base.sh SPARSE 
#./gcc_base.sh SILO 

./linux_base.sh RAM
#./linux_base.sh DDFS 
#./linux_base.sh EXBIN 
#./linux_base.sh SPARSE 
#./linux_base.sh SILO 

./vmdk_base.sh RAM
#./vmdk_base.sh DDFS 
#./vmdk_base.sh EXBIN 
#./vmdk_base.sh SPARSE 
#./vmdk_base.sh SILO 

./tsinghua_base.sh RAM
#./tsinghua_base.sh DDFS 
#./tsinghua_base.sh EXBIN 
#./tsinghua_base.sh SPARSE 
#./tsinghua_base.sh SILO 
