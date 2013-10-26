export DATA_ROOT=/tmp/dtahara/data
export PG_ROOT=~/pgsql

for file in `ls document`; do
  for i in 1 2 3 4; do
    $PG_ROOT/bin/postmaster -D /tmp/dtahara/data >logfile 2>&1 &
    sleep 10
    $PG_ROOT/bin/psql test -f document/$file | grep '^Time:' >> /tmp/dtahara/$file.out
    kill -9 `cat $DATA_ROOT/postmaster.pid | head -n 1`
    sleep 10
    sudo /usr/local/bin/clear-cache.sh
  done
done
