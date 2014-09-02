#!/bin/bash

if [ $# -gt 1 ];then
    echo "dataset <- $1"
    dataset=$1
    echo "sampling <- $2"
    sampling=$2
else
    echo "2 parameters are required"
    exit 1
fi

kernel_path="/home/dataset/kernel_8k/"
vmdk_path="/home/dataset/vmdk_4k/"
rdb_path="/home/dataset/rdb_4k/"
synthetic_path="/home/dataset/synthetic_8k/"

# path: where trace files locate
case $dataset in
    "kernel") 
        path=$kernel_path
        ;;
    "vmdk")
        path=$vmdk_path
        ;;
    "rdb") 
        path=$rdb_path
        ;;
    "synthetic") 
        path=$synthetic_path
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

# r is the sampling Ratio
for r in 1 16 32 64 128 256;do
../rebuild
for file in $(ls $path);do
    ../destor $path/$file -p"fingerprint-index near-exact physical" -p"fingerprint-index-sampling-method $sampling $r" >> log
done
../destor -s >> backup.log
done
