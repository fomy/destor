#!/bin/sh

rewrite=HBR

for param in 0.3 0.4 0.5 0.6 0.7 0.8 0.9
do
./test_rewrite_linux.sh $rewrite $param 0
done

for param in 0.3 0.4 0.5 0.6 0.7 0.8 0.9
do
./test_rewrite_vmdk.sh $rewrite $param 0
done
