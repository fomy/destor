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

echo "rewrite=$1 param=$2 pcache=$3" >> restore.log
echo "lru_cache=512" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=512 --enable_simulator >>log;
i=$(($i+1));
done

echo "lru_cache=384" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=384 --enable_simulator >>log;
i=$(($i+1));
done

echo "lru_cache=256" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=256 --enable_simulator >>log;
i=$(($i+1));
done

echo "lru_cache=192" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=192 --enable_simulator >>log;
i=$(($i+1));
done

echo "lru_cache=128" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=128 --enable_simulator >>log;
i=$(($i+1));
done

echo "lru_cache=96" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=96 --enable_simulator >>log;
i=$(($i+1));
done

echo "lru_cache=64" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=64 --enable_simulator >>log;
i=$(($i+1));
done

echo "lru_cache=32" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=32 --enable_simulator >>log;
i=$(($i+1));
done

echo "#lru_cache=16" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=16 --enable_simulator >>log;
i=$(($i+1));
done

echo "lru_cache=8" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=8 --enable_simulator >>log;
i=$(($i+1));
done

echo "lru_cache=4" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=4 --enable_simulator >>log;
i=$(($i+1));
done
