#!/bin/bash

if [ $# -gt 0 ];then
    echo "dataset <- $1"
    dataset=$1
else
    echo "1 parameters are required"
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
for p in 1 2 4;do
../rebuild
for file in $(ls $path);do
    ../destor $path/$file -p"fingerprint-index near-exact logical" -p"fingerprint-index-segment-algorithm content-defined 1024" -p"fingerprint-index-sampling-method random 128" -p"fingerprint-index-segment-selection base" -p"fingerprint-index-segment-prefetching $p" >> log
done
../destor -s >> backup.log
done

for s in 1 2 4;do
for p in 1 2 4;do
for v in 1 2 4;do
../rebuild
for file in $(ls $path);do
    ../destor $path/$file -p"fingerprint-index near-exact logical" -p"fingerprint-index-segment-algorithm content-defined 1024" -p"fingerprint-index-sampling-method random 128" -p"fingerprint-index-segment-selection top $s" -p"fingerprint-index-segment-prefetching $p" -p"fingerprint-index-value-length $v" >> log
done
../destor -s >> backup.log
done
done
done
