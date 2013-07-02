#! /bin/sh

#if [ $# -lt 1 ]; then
#echo "invalid parameters"
#exit
#fi

index=RAM
rewrite=HBR
dataset=/home/dataset/vmdk_trace

./rebuild
echo "index ${index} rewrite $rewrite" >>  backup.log
jobid=0
for file in $(ls $dataset); do
destor $dataset/$file --index=$index --rewrite=$rewrite >>log
destor $dataset/$file --index=$index --rewrite=$rewrite >>log
jobid=$(($jobid+2))
done

destor -s >> backup.log

for cache_size in 32 64 128 256 512 1024 2048;do
echo "opt $cache_size" >> restore.log
i=0
while [ $i -lt $jobid ]
do
destor -r$i /home/fumin/restore/ --cache=OPT --cache_size=$cache_size >>log
i=$(($i+2))
done
done
