#!/bin/sh

# sed -e 's/"/""/g' < /tmp/dtahara/nb.out | sed -e 's/^{/"{/g' | sed -e
# 's/,$/"/g' | sed -e '1d' | sed -e '$d' > /tmp/dtahara/nb.esc.out
# rm /tmp/dtahara/nb.out
# 
export PG_ROOT=~/pgsql/bin
export OUT=/tmp/dtahara/out
# mkdir $OUT

# echo "\\timing \\\\ Create table test(id serial, data document); copy test(data) from '/tmp/dtahara/nb.esc.out' csv delimiter '|'" | $PG_ROOT/psql test | grep '^Time:' >> $OUT/load

# $PG_ROOT/psql test -f upgrade_num >> $OUT/upgrade_num

echo "Vacuuming"
$PG_ROOT/psql test -c 'vacuum full'
echo "End vacuum"

for query in 'num'; do # str1
  echo "QUERY $query"
  for i in 1 2 3 4; do
    ./execute_query.sh $query

    ./execute.sh upgrade_parallel &
    pid="$!"
    sleep 5
    ./execute_query.sh $query
    wait $pid

    ./execute.sh downgrade_parallel
  done
  echo "==========="
  echo
done
