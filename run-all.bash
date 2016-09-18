#!/usr/bin/env bash
# runs series of 3 tests for // queries
#

start_time=`date +%s`

cd ./fmi/
./run.bash
cd ../pgparallel/
./run.bash
cd ../par_psql/
./run.bash

end_time=`date +%s`

echo "Total Execution time: `expr $end_time - $start_time` s."