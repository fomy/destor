#!/bin/sh

rewrite=CBR

for cache_size in 32 28 24 20 16 12 10 8 6 4 2 0
do
for param in 0.01 0.02 0.03 0.04 0.05 0.06 0.07 0.08 
do
./test_rewrite_linux.sh $rewrite $param $cache_size
done
done

for cache_size in 512 384 256 192 128 96 64 32 16 8 0
do
for param in 0.01 0.02 0.03 0.04 0.05 0.06 0.07 0.08 
do
./test_rewrite_vmdk.sh $rewrite $param $cache_size
done
done
