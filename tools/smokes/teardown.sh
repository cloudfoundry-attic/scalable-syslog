#!/usr/bin/env bash
set -ex

for i in `seq 1 $NUM_APPS`; do
    cf delete drainspinner-$i -r -f
    rm "output-$i.txt" || true
done;

cf delete-service ss-smoke-syslog-${DRAIN_TYPE}-drain-${DRAIN_VERSION} -f
cf delete ${DRAIN_TYPE}-drain -r -f
