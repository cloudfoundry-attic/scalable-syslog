#!/usr/bin/env bash
set -ex

# default job_name to $DRAIN_TYPE-drain
job_name="${JOB_NAME:-$DRAIN_TYPE-drain}"

# restart logspinner apps (writers)
for i in `seq 1 $NUM_APPS`; do
    cf restart "drainspinner-$job_name-$i"
    rm "output-$i.txt" || true
done;

# restart the drain app (reader)
cf restart "$job_name"
