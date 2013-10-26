#!/bin/sh

sed -e 's/"/""/g' < /tmp/dtahara/nb.out | sed -e 's/^{/"{/g' | sed -e 's/,$/"/g' > /tmp/dtahara/nb.esc.out
rm /tmp/dtahara/nb.out

export PG_ROOT=~/pgsql/bin
export OUT=/tmp/dtahara/out
mkdir $OUT
echo "\\timing \\\\ Create table test4(id serial, data document); copy test4(data) from '/tmp/dtahara/nb.esc.out' csv delimiter '|'" | $PG_ROOT/psql test | grep '^Time:' >> $OUT/load

$PG_ROOT/psql test -f upgrade >> $OUT/upgrade

for query in `ls queries`; do
  echo "QUERY $query"
  for i in 1 2 3 4; do
    $PG_ROOT/psql test -f queries/$query | grep '^Time:' >> $OUT/$query.out
  done
  echo "==========="
  echo
done
