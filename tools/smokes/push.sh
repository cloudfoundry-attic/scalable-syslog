#!/usr/bin/env bash
set -exu

source shared.sh

function ensure_counter_app {
    if ! cf app "$counter_name" > /dev/null; then
        push_counter_app
    else
        restart_counter_app
    fi
}

function push_counter_app {
    pushd counter
        if ! [ -e ./counter ]; then
            GOOS=linux go build
        fi
        cf push "$counter_name" -c ./counter -b binary_buildpack -m 128M
    popd
}

function restart_counter_app {
    cf restart "$counter_name"
}

function ensure_drain_app {
    if ! cf app "$job_name" > /dev/null; then
        push_drain_app
    else
        restart_drain_app
    fi
    if ! cf service "ss-smoke-syslog-$job_name-drain-$DRAIN_VERSION" 2> /dev/null; then
        create_drain_service
    fi
}

function ensure_spinner_apps {
    for i in $(seq 1 "$NUM_APPS"); do
        if ! cf app "drainspinner-$job_name-$i" 2> /dev/null; then
            push_spinner_app "$i"
        else
            restart_spinner_app "$i"
        fi
    done
}

function push_drain_app {
    pushd "./${DRAIN_TYPE}_drain"
        if ! [ -e "./${DRAIN_TYPE}_drain" ]; then
            GOOS=linux go build
        fi
        cf push "$job_name" -c "./${DRAIN_TYPE}_drain" -b binary_buildpack --no-route --no-start -m 128M
        cf set-env "$job_name" COUNTER_URL "https://$(app_url $counter_name)"

        if [ "$DRAIN_TYPE" = "syslog" ]; then
            cf map-route "$job_name" "$CF_APP_DOMAIN" --random-port
        else
            cf map-route "$job_name" "$CF_APP_DOMAIN" --hostname "$job_name"
        fi

        cf start "$job_name"
    popd
}

function restart_drain_app {
    cf restart "$job_name"
}

function create_drain_service {
    drain_domain=$(cf app "$job_name" | grep -E 'routes|urls' | awk '{print $2}')
    cf create-user-provided-service \
        "ss-smoke-syslog-$job_name-drain-$DRAIN_VERSION" \
        -l "$DRAIN_TYPE://$drain_domain/drain?drain-version=$DRAIN_VERSION" || true
}

function push_spinner_app {
    pushd ../logspinner
        if ! [ -e ./logspinner ]; then
            GOOS=linux go build
        fi
        for i in {1..5}; do
            cf push "drainspinner-$job_name-$1" -c ./logspinner -b binary_buildpack -m 128M && break || sleep 5
        done
        cf bind-service \
            "drainspinner-$job_name-$1" \
            "ss-smoke-syslog-$job_name-drain-$DRAIN_VERSION"
    popd
}

function restart_spinner_app {
    cf restart "drainspinner-$job_name-$1"
}

function login {
    cf login \
        -a api."$CF_SYSTEM_DOMAIN" \
        -u "$CF_USERNAME" \
        -p "$CF_PASSWORD" \
        -s "$CF_SPACE" \
        -o "$CF_ORG" \
        --skip-ssl-validation # TODO: consider passing this in as a param
}

function main {
    # default job_name to $DRAIN_TYPE-drain
    job_name="${JOB_NAME:-$DRAIN_TYPE-drain}"
    counter_name="$job_name-counter"

    login
    ensure_counter_app
    ensure_drain_app
    ensure_spinner_apps
}
main
