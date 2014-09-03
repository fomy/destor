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

kernel_fcs=(256 512 1024 2048 4096)
vmdk_fcs=(256 512 1024 2048 4096)
rdb_fcs=(256 512 1024 2048 4096)
synthetic_fcs=(256 512 1024 2048 4096)

# path: where trace files locate
# fcs: the restore cache size
case $dataset in
    "kernel") 
        path=$kernel_path
        fcs=(${kernel_fcs[@]})
        ;;
    "vmdk")
        path=$vmdk_path
        fcs=(${vmdk_fcs[@]})
        ;;
    "rdb") 
        path=$rdb_path
        fcs=(${rdb_fcs[@]})
        ;;
    "synthetic") 
        path=$synthetic_path
        fcs=(${synthetic_fcs[@]})
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

for s in ${fcs[@]};do
../rebuild
for file in $(ls $path);do
    ../destor $path/$file -p"fingerprint-index exact physical" -p"fingerprint-index-cache-size $s" >> log
done
../destor -s >> backup.log
done
