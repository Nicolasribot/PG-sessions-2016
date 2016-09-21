#!/usr/bin/env bash

for i in 5 ;do
    for j in 8; do
        start_time=`date +%s`
        rm tmptest1.sh
        sed -e "s/%SPLIT%/${i}/g" -e "s/%JOBS%/${j}/g" test1.sh > tmptest1.sh
        chmod a+x tmptest1.sh
        head -16 tmptest1.sh | tail -2
        ./tmptest1.sh
        end_time=`date +%s`
        echo "FMI Execution time: (split: ${i}, jobs: ${j}): `expr $end_time - $start_time` s."
    done
done
