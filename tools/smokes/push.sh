#!/usr/bin/env bash
set -exu

function ensure_drain_app {
    if ! cf app "$job_name"; then
        push_drain_app
    fi
}

function ensure_spinner_apps {
    for i in $(seq 1 "$NUM_APPS"); do
        if ! cf app "drainspinner-$job_name-$i"; then
            push_spinner_app "$i"
        fi
    done
}

function push_drain_app {
    pushd "./${DRAIN_TYPE}_drain"
        GOOS=linux go build
        cf push "$job_name" -c "./${DRAIN_TYPE}_drain" -b binary_buildpack --no-route
        if [ "$DRAIN_TYPE" = "syslog" ]; then
            cf map-route "$job_name" "$CF_APP_DOMAIN" --random-port
        else
            cf map-route "$job_name" "$CF_APP_DOMAIN" --hostname "$job_name"
        fi
        drain_domain=$(cf app "$job_name" | grep urls | awk '{print $2}')
        cf create-user-provided-service \
            "ss-smoke-syslog-$job_name-drain-$DRAIN_VERSION" \
            -l "$DRAIN_TYPE://$drain_domain/drain?drain-version=$DRAIN_VERSION" || true
    popd
}

function push_spinner_app {
    pushd ../logspinner
        if ! [ -e ./logspinner ]; then
            GOOS=linux go build
        fi
        cf push "drainspinner-$job_name-$1" -c ./logspinner -b binary_buildpack
        cf bind-service \
            "drainspinner-$job_name-$1" \
            "ss-smoke-syslog-$job_name-drain-$DRAIN_VERSION"
    popd
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

    login
    ensure_drain_app
    ensure_spinner_apps
}
main
