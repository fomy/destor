#! /bin/sh
# ./test_index_rewrite.sh <index_type> <rewrite> <dataset> 

echo $#
if [ $# -lt 3 ]; then
echo "invalid parameters"
exit
fi

./rebuild

index=$1
rewrite=$2

echo "#index ${index} rewrite ${rewrite}" >>  backup.log
jobid=0
for file in $(ls $3); do
echo 3 > /proc/sys/vm/drop_caches
destor $3/$file --index=$index --rewrite=$rewrite >>log
jobid=$(($jobid+1))
done

destor -s >> backup.log

echo "#index=${index}, rewrite=${rewrite}" >>  restore.log
echo "#lru cache 100" >> restore.log
i=0
while [ $i -le $jobid ]
do
echo 3 > /proc/sys/vm/drop_caches
destor -r$i /home/fumin/restore/ --cache=LRU --cache_size=100 >>log
i=$(($i+1))
done

echo "#opt cache 100" >> restore.log
i=0
while [ $i -le $jobid ]
do
echo 3 > /proc/sys/vm/drop_caches
destor -r$i /home/fumin/restore/ --cache=OPT --cache_size=100 >>log
i=$(($i+1))
done

echo "#asm cache 100" >> restore.log
i=0
while [ $i -le $jobid ]
do
echo 3 > /proc/sys/vm/drop_caches
destor -r$i /home/fumin/restore/ --cache=ASM --cache_size=100 >>log
i=$(($i+1))
done

