#!/bin/sh

export MONGO_ROOT=~/mongodb/bin
export DATA=/tmp/dtahara/data
export OUT=/tmp/dtahara/out

mkdir -p $OUT
mkdir -p $DATA

sudo /usr/local/bin/clear-cache.sh

time $MONGO_ROOT/mongoimport --collection=test --file=/tmp/dtahara/nb.out >> $OUT/time.out

# execute a query to warm cache
time $MONGO_ROOT/mongo queries/1.js >> $OUT/warm.out

for query in `ls queries`; do
    echo "Printing times for $query"
    for i in 1 2 3 4; do
        time $MONGO_ROOT/mongo queries/$query >> $OUT/$query.out
    done
    echo "=============="
done
