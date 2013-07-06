#! /bin/sh

if [ $# -lt 1 ]; then
echo "invalid parameters"
exit
fi

index=RAM
rewrite=$1
dataset=/home/dataset/linux_src_trace

./rebuild
echo "index ${index} rewrite $rewrite" >>  backup.log
echo "rewrite $rewrite" >>  delete.log
jobid=0
for file in $(ls $dataset); do
destor $dataset/$file --index=$index --rewrite=$rewrite >>log
if [ $jobid -ge 10 ]; then
destor -d$(($jobid-10))
fi
jobid=$(($jobid+1))
done

destor -s >> backup.log
