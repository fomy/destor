#!/bin/sh

rewrite=CAP

for cache_size in 20 12 10 8 6 4 2 0
do
for param in 6 10 14 18 22 26 
do
./test_rewrite_linux.sh $rewrite $param $cache_size
done
done

for cache_size in 256 128 64 32 16 8 0
do
for param in 6 10 14 18 22 26 
do
./test_rewrite_vmdk.sh $rewrite $param $cache_size
done
done
