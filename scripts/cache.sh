#!/bin/bash

dataset="kernel"

if [ $# -gt 0 ];then
    echo "dataset <- $1"
    dataset=$1
else
    echo "default dataset <- $dataset"
fi

kernel_path="/home/dataset/kernel_8k/"
vmdk_path="/home/dataset/vmdk_4k/"
rdb_path="/home/dataset/rdb_4k/"
synthetic_path="/home/dataset/synthetic_8k/"

# path: where trace files locate
# fcs: the restore cache size
case $dataset in
    "kernel") 
        path=$kernel_path
        rcs=32
        ;;
    "vmdk")
        path=$vmdk_path
        rcs=256
        ;;
    "rdb") 
        path=$rdb_path
        rcs=64
        ;;
    "synthetic") 
        path=$synthetic_path
        rcs=64
        ;;
    *) 
        echo "Wrong dataset!"
        exit 1
        ;;
esac

# split the restore.log according to the restore cache size
split_file(){
    lines=$(cat $1) # read the file
    IFS=$'\n' # split 'lines' by '\n'
    lineno=0
    for line in $lines; do
        if [ $(($lineno%3)) -eq 0 ];then
            echo $line >> restore.lru.log
        elif [ $(($lineno%3)) -eq 1 ];then
            echo $line >> restore.opt.log
        else
            echo $line >> restore.asm.log
        fi
        lineno=$(($lineno+1))
    done
}

# ./rebuild would clear data of previous experiments
# ./destor executes a backup job
#   (results are written to backup.log)
# ./destor -rN executes a restore job under various restore cache size
#   (results are written to restore.log)
i=0
../rebuild
for file in $(ls $path);do
    ../destor $path/$file -p"fingerprint-index exact physical" >> log
    ../destor -r$i /home/fumin/restore -p"restore-cache lru $rcs" >> log
    ../destor -r$i /home/fumin/restore -p"restore-cache opt $rcs" >> log
    ../destor -r$i /home/fumin/restore -p"restore-cache asm $rcs" >> log
    i=$(($i+1))
done
../destor -s >> backup.log

split_file restore.log
rm restore.log

i=0
../rebuild
for file in $(ls $path);do
    ../destor $path/$file -p"fingerprint-index exact physical" -p"rewrite-enable-har yes" -p"rewrite-har-utilization-threshold 0.5" >> log
    ../destor -r$i /home/fumin/restore -p"restore-cache lru $rcs" >> log
    ../destor -r$i /home/fumin/restore -p"restore-cache opt $rcs" >> log
    ../destor -r$i /home/fumin/restore -p"restore-cache asm $rcs" >> log
    i=$(($i+1))
done
../destor -s >> backup.log

split_file restore.log
