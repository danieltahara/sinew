#!/usr/bin/zsh

export SRC_ROOT=`pwd`/src
export PG_ROOT=$SRC_ROOT/../../postgresql-9.3beta2

rm -rf $PG_ROOT/contrib/document
cp -r src/postgres/document $PG_ROOT/contrib/document || exit 0
(cd $PG_ROOT/contrib/document;
   (cd lib/jsmn; make) && make && sudo make install)

