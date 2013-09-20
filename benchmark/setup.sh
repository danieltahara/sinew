export SRC_ROOT=`pwd`

# Install Postgres, etc
SRC_ROOT/../install.sh

# Pl/v8 stuff

git clone git://github.com/v8/v8.git v8 && cd v8
export GYPFLAGS="-D OS=freebsd"
make dependencies
make native.check -j 4 library=shared strictaliasing=off console=readline
cp v8.so /usr/lib/v8.so
cd $SRC_ROOT

# TODO: Add pg_config to you $PATH. Normally pg_config exists in $PGHOME/bin.

git clone https://code.google.com/p/plv8js/ && cd plv8js
make && make install

cd $SRC_ROOT
