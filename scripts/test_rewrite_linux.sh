#! /bin/bash
# ./test_rewrite_linux.sh <rewrite> <param> <predicted cache size> 

echo $#
if [ $# -lt 3 ]; then
echo "invalid parameters"
exit
fi

./rebuild

dataset=/home/fumin/linux_txt

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
echo "cache=32" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=32 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=28" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=28 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=24" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=24 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=20" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=20 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=16" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=16 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=12" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=12 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=10" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=10 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=8" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=8 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=6" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=6 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=4" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=4 --simulation=RECOVERY >>log;
i=$(($i+1));
done

echo "cache=2" >> restore.log
i=0
while [ $i -le $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=$cache_type --cache_size=2 --simulation=RECOVERY >>log;
i=$(($i+1));
done
done
