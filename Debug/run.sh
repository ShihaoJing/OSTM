#!/usr/bin/env bash
TARGET=./OSTM

nthreads=1

while [ $nthreads -le 2 ]
do
    echo "Running at threads=" $nthreads
    i=0
    while [ $i -le 5 ]
    do 
        $TARGET $nthreads inputs/in_1M.txt
        i=$((i+1))
        sleep 1
    done
    echo "==============================="
    nthreads=$((nthreads * 2))
    echo ""
    sleep 1
done 