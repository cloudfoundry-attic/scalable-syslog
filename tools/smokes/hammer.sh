#!/usr/bin/env bash
set -exu

source ./shared.sh

job_name="${JOB_NAME:-$DRAIN_TYPE-drain}"

for i in `seq 1 $NUM_APPS`; do
    rm "output-$i.txt" || true

    cf logs "drainspinner-$job_name-$i" > "output-$i.txt" 2>&1 &
done

sleep 80

echo "Begin the hammer"
for i in $(seq 1 "$NUM_APPS"); do
    curl "$(app_url "drainspinner-$job_name-$i")?cycles=${CYCLES}&delay=${DELAY_US}us" &> /dev/null
done

sleep 25
