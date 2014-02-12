#!/bin/sh

export PG_ROOT=~/pgsql/bin
export OUT=/tmp/dtahara/out

$PG_ROOT/psql test -f $1 | grep '^Time:' >> $OUT/$1.out
