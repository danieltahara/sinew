export SRC_ROOT=`pwd`
export DATA_ROOT=~/hadapt/results
export PG_ROOT=~/pgsql

rm -rf $DATA_ROOT
mkdir $DATA_ROOT
mkdir $DATA_ROOT/out
cp -r $SRC_ROOT/queries/ $DATA_ROOT/queries
cp -r $SRC_ROOT/ddl/ $DATA_ROOT/ddl

$PG_ROOT/bin/psql test -e -f $DATA_ROOT/ddl/json/drop
# $PG_ROOT/bin/postmaster -D /tmp/dtahara/data >logfile 2>&1 &
sleep 10
for type in document; do
  for size in test11; do
    $PG_ROOT/bin/psql test -e -f $DATA_ROOT/ddl/$type/create
    mkdir -p $DATA_ROOT/out/$type/$size

    # Perform Load
    $PG_ROOT/bin/psql test -e -f $DATA_ROOT/ddl/$type/load_$size >> $DATA_ROOT/out/$type/$size/load
    $PG_ROOT/bin/psql test -e -f $DATA_ROOT/ddl/$type/size >> $DATA_ROOT/out/$type/$size/size

    for query in `ls $DATA_ROOT/queries/$type`; do
      for i in 1 2 3 4; do
        # Start server
        # $PG_ROOT/bin/postmaster -D $PG_ROOT/data >logfile 2>&1 &
        # sleep 10
        # Execute query
        $PG_ROOT/bin/psql test -e -f $DATA_ROOT/queries/$type/$query 2>&1 >> $DATA_ROOT/out/$type/$size/$query
        # Kill server
        # kill -9 `cat $PG_ROOT/data/postmaster.pid | head -n 1`
        # sleep 10
        # sync && echo 3 > /proc/sys/vm/drop_caches
        # Clear caches
      done
    done
    $PG_ROOT/bin/psql test -e -f $DATA_ROOT/ddl/$type/drop
  done
done
