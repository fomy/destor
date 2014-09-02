#!/bin/bash

if [ $# -gt 2 ];then
    echo "dataset <- $1"
    dataset=$1
    echo "sampling <- $3"
    sampling=$3
else
    echo "3 parameters are required"
    exit 1
fi

case $2 in
    "cds")
        echo "segmenting <- content-defined"
        segmenting="content-defined"
        ;;
    "fixed")
        echo "segmenting <- fixed"
        segmenting="fixed"
        ;;
    *)
        echo "wrong segmenting method!"
        exit 1
        ;;
esac

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
    ../destor $path/$file -p"fingerprint-index exact logical" -p"fingerprint-index-segment-algorithm $segmenting 1024" -p"fingerprint-index-sampling-method $sampling $r" >> log
done
../destor -s >> backup.log
done
