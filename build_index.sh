#!/bin/bash
##  building index
set -u
set -o pipefail

path="./data/index_data_tmp"

rm -rf $path/*
echo "clean old index info done"

echo "now mkdir ..."

seq=0
while((seq < 128));
do
    mkdir -p $path/$seq
    seq=$(($seq + 1))
done

echo "all ready, begin building index..."

go run index.go "questions.100.utf8"

echo "all done"
