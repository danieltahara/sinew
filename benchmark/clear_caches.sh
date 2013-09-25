export DATA_ROOT=/tmp/dtahara/data
export PG_ROOT=~/pgsql

for i in 1 2 3 4; do
  $PG_ROOT/bin/postmaster -D /tmp/dtahara/data >logfile 2>&1 &
  sleep 10
  time echo "\\timing \\\\ select document_get_text(data, 'text') from test;" | $PG_ROOT/bin/psql test | grep '^Time:' >> /tmp/dtahara/doc.out
  kill -9 `cat $DATA_ROOT/postmaster.pid | head -n 1`
  sleep 10
  sudo /usr/local/bin/clear-cache.sh
done

for i in 1 2 3 4; do
  $PG_ROOT/bin/postmaster -D $DATA_ROOT > /tmp/logfile 2>&1 &
  sleep 10
  time echo "\\timing \\\\ select data->'text' from test2;" | $PG_ROOT/bin/psql test | grep '^Time:' >> /tmp/dtahara/json.out
  kill -9 `cat $DATA_ROOT/postmaster.pid | head -n 1`
  sleep 10
  sudo /usr/local/bin/clear-cache.sh
done
