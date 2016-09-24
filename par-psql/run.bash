#!/usr/bin/env bash
#
#
start_time=`date +%s`

/usr/local/pgsql-9.6/bin/par_psql -p 5439 -d nicolas --file=./test1.sql

end_time=`date +%s`

echo "Par-psql Execution time: `expr $end_time - $start_time` s."