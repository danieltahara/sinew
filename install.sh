#!/usr/bin/sh

export SRC_ROOT=`pwd`/src
export PG_ROOT=$SRC_ROOT/../../postgresql-9.3beta2

cp -r src/document $PG_ROOT/contrib/document &&
(cd $PG_ROOT/contrib/document; make)

