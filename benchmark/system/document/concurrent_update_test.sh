#!/bin/sh

sed -e 's/"/""/g' < /tmp/dtahara/nb.out | sed -e 's/^{/"{/g' | sed -e 's/,$/"/g' > /tmp/dtahara/nb.esc.out
rm /tmp/dtahara/nb.out

export PG_ROOT=~/pgsql/bin
export OUT=/tmp/dtahara/out
mkdir $OUT
echo "\\timing \\\\ Create table test(id serial, data document); copy test(data) from '/tmp/dtahara/nb.esc.out' csv delimiter '|'" | $PG_ROOT/psql test | grep '^Time:' >> $OUT/load

$PG_ROOT/psql test -f upgrade_num >> $OUT/upgrade_num


for query in 'str1' 'num'; do
  echo "QUERY $query"
  for i in 1 2 3 4; do
    ./parallel_commands "$PG_ROOT/psql test -f queries/$query | grep '^Time:' >> $OUT/$query.out" \
      "$PG_ROOT/psql test -f upgrade_parallel | grep '^Time:' >> $OUT/downgrade.out"
    $PG_ROOT/psql test -f downgrade_parallel | grep '^Time:' >> $OUT/downgrade.out
  done
  echo "==========="
  echo
done
