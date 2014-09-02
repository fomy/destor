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

kernel_rcs=(4 8 16 32 64 128 256 )
vmdk_rcs=(64 128 256 512 1024 2048 4096)
rdb_rcs=(8 16 32 64 128 256 512)
synthetic_rcs=(8 16 32 64 128 256 512)

# path: where trace files locate
# rcs: the restore cache size
case $dataset in
    "kernel") 
        path=$kernel_path
        rcs=(${kernel_rcs[@]})
        ;;
    "vmdk")
        path=$vmdk_path
        rcs=(${vmdk_rcs[@]})
        ;;
    "rdb") 
        path=$rdb_path
        rcs=(${rdb_rcs[@]})
        ;;
    "synthetic") 
        path=$synthetic_path
        rcs=(${synthetic_rcs[@]})
        ;;
    *) 
        echo "Wrong dataset!"
        exit 1
        ;;
esac

# ./rebuild would clear data of previous experiments
# ./destor executes a backup job
#   (results are written to backup.log)
# ./destor -rN executes a restore job under various restore cache size
#   (results are written to restore.log)

for c in ${rcs[@]};do
n=0
../rebuild
for file in $(ls $path);do
    ../destor $path/$file -p"restore-cache lru $c" >> log
    ../destor -r$n /home/fumin/restore -p"restore-cache lru $c" >> log
    ../destor -r$n /home/fumin/restore -p"restore-cache opt $c" >> log
    n=$(($n+1))
done
../destor -s >> backup.log

done

# split the restore.log according to the restore cache size
split_file(){
    lines=$(cat $1) # read the file
    IFS=$'\n' # split 'lines' by '\n'
    lineno=0
    for line in $lines; do
        index=$(( ($lineno)%2 ))
        if [ $(($lineno%2)) -eq 0 ];then
            echo $line >> restore.lru.log
        else
            echo $line >> restore.opt.log
        fi
        lineno=$(($lineno+1))
    done
}

split_file restore.log

