#!/usr/bin/env bash
set -eu

source ./shared.sh

function hammer_url {
    echo "$(app_url "$(drainspinner_app_name)")?cycles=${CYCLES}&delay=${DELAY_US}us&text=live"
}

function establish_logs_stream {
    checkpoint "Starting App Logs Stream"

    cf logs "$(drainspinner_app_name)" > output.txt 2>&1 &
    local wait=10
    echo "sleeping for ${wait}s to wait for log stream to be established"
    sleep "$wait"
}

function hammer {
    checkpoint "Writing messages"

    curl "$(hammer_url)" &> /dev/null

    export -f block_until_count_equals_cycles
    if ! timeout 180s bash -ec "block_until_count_equals_cycles"; then
        warning "timed out waiting for all the messages to be received"
    fi
}

function block_until_count_equals_cycles {
    source ./shared.sh
    while true; do
        local count=$(curl -s "$(app_url "$(counter_app_name)")/get")
        if [ "${count:-0}" -ge "$CYCLES" ]; then
            success "received all messages"
            break
        fi
        echo "waiting to receive all messages, current count $count"
        sleep 5
    done
    exit 0
}

function main {
    checkpoint "Starting Hammer"

    login
    establish_logs_stream
    hammer
}
main
