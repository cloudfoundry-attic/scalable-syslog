#!/usr/bin/env bash
set -eu

source ./shared.sh

function main {
    checkpoint "Tearing Down Apps and Services"

    login
    cf delete "$(drainspinner_app_name)" -r -f
    cf delete "$(drain_app_name)" -r -f
    cf delete "$(counter_app_name)" -r -f
    cf delete-service "$(syslog_drain_service_name)" -f
}
main
