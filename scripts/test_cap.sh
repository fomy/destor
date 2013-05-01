#!/bin/sh

rewrite=CAP

for cache_size in 32 28 24 20 16 12 10 8 6 4 2 0
do
for param in 20 18 16 14 12 10 8 6 
do
./test_rewrite_linux.sh $rewrite $param $cache_size
done
done

for cache_size in 512 384 256 192 128 96 64 32 16 8 0 
do
for param in 20 18 16 14 12 10 8 6 
do
./test_rewrite_vmdk.sh $rewrite $param $cache_size
done
done
