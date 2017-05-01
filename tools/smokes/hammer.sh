#!/usr/bin/env bash
set -exu

function app_url {
    local guid=$(cf app "$1" --guid)
    local route_data=$(cf curl "/v2/apps/$guid/routes")
    local domain_url=$(echo "$route_data" | jq .resources[0].entity.domain_url --raw-output)
    local domain_name=$(cf curl "$domain_url" | jq .entity.name --raw-output)

    local port=$(echo "$route_data" | jq .resources[0].entity.port --raw-output)
    if [ "$port" != "null" ]; then
        # this app uses tcp routing
        echo "$domain_name:$port"
        return 0
    fi
    local host=$(echo "$route_data" | jq .resources[0].entity.host --raw-output)
    echo "$host.$domain_name"
}

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
