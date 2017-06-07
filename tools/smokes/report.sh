#!/usr/bin/env bash
set -eu

source ./shared.sh

function kill_cf {
    pkill cf || true
}

function datadog_url {
    echo "https://app.datadoghq.com/api/v1/series?api_key=$DATADOG_API_KEY"
}

function post_to_datadog {
    local payload=$(cat <<JSON
{
    "series": [{
        "metric": "smoke_test.ss.loggregator.$1",
        "points": [[$2, $3]],
        "type": "gauge",
        "host": "$CF_SYSTEM_DOMAIN",
        "tags": [
            "drain_version:$DRAIN_VERSION",
            "drain_type:$DRAIN_TYPE",
            "job_name:$(job_name)"
        ]
    }]
}
JSON
)
    curl -X POST -H "Content-type: application/json" -d "$payload" "$(datadog_url)"
}

function main {
    checkpoint "Reporting Results"

    kill_cf
    login

    msg_count=0
    c=$(grep -c live output.txt)
    : $(( msg_count = msg_count + c ))

    drain_msg_count=$(curl -s "$(app_url "$(counter_app_name)")/get")
    currenttime=$(date +%s)

    post_to_datadog "msg_count" "$currenttime" "$msg_count"
    post_to_datadog "drain_msg_count" "$currenttime" "$drain_msg_count"
    post_to_datadog "delay" "$currenttime" "$DELAY_US"
    post_to_datadog "cycles" "$currenttime" "$CYCLES"

    if [ "$msg_count" -eq 0 ]; then
        error "message count was zero, sad"
        exit 1
    fi
    if [ "$drain_msg_count" -eq 0 ]; then
        error "drain count was zero, sad"
        exit 1
    fi
}
main
