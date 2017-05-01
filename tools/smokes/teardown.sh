#!/usr/bin/env bash
set -ex

# default job_name to $DRAIN_TYPE-drain
job_name="${JOB_NAME:-$DRAIN_TYPE-drain}"
counter_name="$job_name-counter"

# delete logspinner apps (writers)
for i in `seq 1 $NUM_APPS`; do
    cf delete "drainspinner-$job_name-$i" -r -f
    rm "output-$i.txt" || true
done;

# delete the drain app (reader)
cf delete-service "ss-smoke-syslog-$job_name-drain-${DRAIN_VERSION}" -f
cf delete "$job_name" -r -f
cf delete "$counter_name" -r -f
