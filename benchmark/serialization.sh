export SRC_ROOT=`pwd`
export DATA_ROOT=/tmp/benchmark/data

# Install Postgres, etc
SRC_ROOT/install.sh

mkdir $DATA_ROOT
mkdir $DATA_ROOT/out
cp -r $SRC_ROOT/benchmark/queries/ $DATA_ROOT/queries

for type in "json", "document"; do
  su -c postgres "/usr/local/pgsql/bin/psql test -e -f $DATA_ROOT/queries/$type/create"

  for size in 800, 8000; do
    su -c postgres "mkdir -p $DATA_ROOT/out/$type/$size"

    # Perform Load
    su -c postgres "/usr/local/pgsql/bin/psql test -e -f $DATA_ROOT/queries/$type/load_$size >> $DATA_ROOT/out/$type/$size/load"

    for query in `ls $DATA_ROOT/queries`; do
      for i in 1,2,3; do
        # Execute query
        su -c postgres "/usr/local/pgsql/bin/psql test -e -f $DATA_ROOT/queries/$type/$query 2>&1 >> $DATA_ROOT/out/$type/$size/$query"
        # Clear caches
        sudo (sync && echo 3 > /proc/sys/vm/drop_caches)
        # Restart server
        su -c postgres "/usr/local/pgsql/bin/postmaster -D /usr/local/pgsql/data >logfile 2>&1 &"
      done
    done

    su -c postgres "/usr/local/pgsql/bin/psql test -e -f $DATA_ROOT/queries/$type/drop"
  done
done
