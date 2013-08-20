#!/usr/bin/zsh

echo "Initializing postgres"
# su postgres -c "/usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data/
# /usr/local/pgsql/bin/postmaster -D /usr/local/pgsql/data > logfile 2>&1 &
# /usr/local/pgsql/bin/createdb test"

/usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data/
/usr/local/pgsql/bin/postmaster -D /usr/local/pgsql/data > logfile 2>&1 &
/usr/local/pgsql/bin/createdb test

