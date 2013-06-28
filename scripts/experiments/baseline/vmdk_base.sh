#! /bin/sh

#if [ $# -lt 1 ]; then
#echo "invalid parameters"
#exit
#fi

./rebuild

index=RAM
rewrite=NO
dataset=/home/dataset/vmdk_trace

echo "index ${index}" >>  backup.log
jobid=0
for file in $(ls $dataset); do
destor $dataset/$file --index=$index --rewrite=$rewrite >>log
jobid=$(($jobid+1))
done

destor -s >> backup.log

echo "index=${index}" >>  restore.log

for cache_size in 32 64 128 256 512 1024; do
echo "lru $cache_size" >> restore.log
i=0
while [ $i -lt $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=$cache_size >>log
i=$(($i+1))
done
done

for cache_size in 32 64 128 256 512 1024; do
echo "asm $cache_size" >> restore.log
i=0
while [ $i -lt $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=ASM --cache_size=$cache_size >>log
i=$(($i+1))
done
done

for cache_size in 32 64 128 256 512 1024; do
echo "opt $cache_size" >> restore.log
i=0
while [ $i -lt $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=OPT --cache_size=$cache_size >>log
i=$(($i+1))
done
done

