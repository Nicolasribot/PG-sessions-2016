#!/usr/bin/env bash


for i in 8 ;do
    start_time=`date +%s`
    sed "s/%WORKERS%/${i}/g" test.sql | /usr/local/pgsql-9.6/bin/psql -p 5439 -d nicolas
    end_time=`date +%s`
    echo "PG // Execution time (${i} workers): `expr $end_time - $start_time` s."
done

