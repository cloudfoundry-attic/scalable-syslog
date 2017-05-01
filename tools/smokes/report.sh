#!/usr/bin/env bash
set -exu

pkill cf || true

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

msg_count=0
for i in `seq 1 $NUM_APPS`; do
    c=$(cat output-$i.txt | grep -c 'msg')
    : $(( msg_count = $msg_count + $c ))
done;

drain_count=$(curl $(app_url "$job_name")/count)
currenttime=$(date +%s)

curl -X POST -H "Content-type: application/json" \
-d "$(cat <<JSON
{
    "series": [{
        "metric": "smoke_test.ss.loggregator.msg_count",
        "points": [[$currenttime, $msg_count]],
        "type": "gauge",
        "host": "$CF_SYSTEM_DOMAIN",
        "tags": [
            "drain_version:$DRAIN_VERSION",
            "drain_type:$DRAIN_TYPE",
            "job_name:$job_name"
        ]
    }]
}
JSON
)" \
"https://app.datadoghq.com/api/v1/series?api_key=$DATADOG_API_KEY"

curl -X POST -H "Content-type: application/json" \
-d "$(cat <<JSON
{
    "series" : [{
        "metric": "smoke_test.ss.loggregator.drain_msg_count",
        "points": [[$currenttime, $drain_count]],
        "type": "gauge",
        "host": "$CF_SYSTEM_DOMAIN",
        "tags": [
            "drain_version:$DRAIN_VERSION",
            "drain_type:$DRAIN_TYPE",
            "job_name:$job_name"
        ]
    }]
}
JSON
)" \
"https://app.datadoghq.com/api/v1/series?api_key=$DATADOG_API_KEY"

curl -X POST -H "Content-type: application/json" \
-d "$(cat <<JSON
{
    "series": [{
        "metric": "smoke_test.ss.loggregator.delay",
        "points": [[$currenttime, $DELAY_US]],
        "type": "gauge",
        "host": "$CF_SYSTEM_DOMAIN",
        "tags": [
            "drain_version:$DRAIN_VERSION",
            "drain_type:$DRAIN_TYPE",
            "job_name:$job_name"
        ]
    }]
}
JSON
)" \
"https://app.datadoghq.com/api/v1/series?api_key=$DATADOG_API_KEY"

curl -X POST -H "Content-type: application/json" \
-d "$(cat <<JSON
{
    "series": [{
        "metric": "smoke_test.ss.loggregator.cycles",
        "points": [[$currenttime, $(expr $CYCLES \* $NUM_APPS)]],
        "type": "gauge",
        "host": "$CF_SYSTEM_DOMAIN",
        "tags": [
            "drain_version:$DRAIN_VERSION",
            "drain_type:$DRAIN_TYPE",
            "job_name:$job_name"
        ]
    }]
}
JSON
)" \
"https://app.datadoghq.com/api/v1/series?api_key=$DATADOG_API_KEY"

if [ "$msg_count" -eq 0 ]; then
    echo message count was zero, sad
    exit 1
fi
