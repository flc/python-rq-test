#!/bin/bash

num=$(nproc)
if [ "$1" != "" ]; then
    let num=$1
fi
seq $num | parallel -j $num --no-notice --progress -v -v "rqworker 2>&1 | tee worker{}.log"
