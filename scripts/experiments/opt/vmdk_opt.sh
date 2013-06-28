#! /bin/sh

index=RAM
rewrite=HBR
dataset=/home/dataset/vmdk_trace

./rebuild
echo "index ${index} rewrite $rewrite" >>  backup.log
jobid=0
for file in $(ls $dataset); do
destor $dataset/$file --index=$index --rewrite=$rewrite >>log
jobid=$(($jobid+1))
done

destor -s >> backup.log

for window_size in 170000 80000 40000 20000 10000;do
for cache_size in 32 64 128 256 512 1024;do
echo "opt $cache_size" >> restore.log
i=0
while [ $i -lt $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=OPT --cache_size=$cache_size --window_size=$window_size >>log
i=$(($i+1))
done
done
done
