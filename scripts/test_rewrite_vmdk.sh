#! /bin/bash
# ./test_rewrite_vmdk.sh <rewrite> <param> <predicted cache size> 

echo $#
if [ $# -lt 3 ]; then
echo "invalid parameters"
exit
fi

./rebuild

dataset=/home/fumin/vm

rewrite="--rewrite=$1"

if [ $1 = "CFL" ]; then
param="--cfl_p=$2"
elif [ $1 = "CBR" ]; then
param="--rewrite_limit=$2"
elif [ $1 = "CAP" ]; then
param="--capping_t=$2"
elif [ $1 = "HBR" ]; then
rewrite="--enable_hbr"
param="--hbr_usage=$2"
fi

if [ $3 -gt 0 ]; then
pcache="--cache_size=$3 --enable_cache_filter"
else
pcache=""
fi

echo "rewrite=$1 param=$2 pcache=$3" >>  backup.log
jobid=0
for file in $(ls $dataset); do
destor $dataset/$file --index=RAM $rewrite $param $pcache >>log;
jobid=$(($jobid+1));
done

jobid=$(($jobid-1));
destor -s >> backup.log;

for cache_type in LRU ASM OPT; do
echo "rewrite=$1 param=$2 pcache=$3 cache=$cache_type" >> restore.log
echo "cache=512" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=512 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=384" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=384 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=256" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=256 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=192" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=192 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=128" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=128 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=96" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=96 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=64" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=64 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=32" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=32 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=16" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=16 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=8" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=8 --simulation=RECOVERY >>log;
i=$(($i+1));
done
done
