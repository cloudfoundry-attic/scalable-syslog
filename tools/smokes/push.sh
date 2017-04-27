#!/usr/bin/env bash
set -exu

cf login \
    -a api."$CF_SYSTEM_DOMAIN" \
    -u "$CF_USERNAME" \
    -p "$CF_PASSWORD" \
    -s "$CF_SPACE" \
    -o "$CF_ORG" \
    --skip-ssl-validation # TODO: consider passing this in as a param

# default job_name to $DRAIN_TYPE-drain
job_name="${JOB_NAME:-$DRAIN_TYPE-drain}"
if cf app "$job_name"; then
    exit 0
fi

# push the drain app (reader)
pushd "./$DRAIN_TYPE_drain"
    GOOS=linux go build
    cf push "$job_name" -c "./$DRAIN_TYPE_drain" -b binary_buildpack --no-route
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

# push logspinner apps (writers)
pushd ../logspinner
    GOOS=linux go build
    for i in $(seq 1 "$NUM_APPS"); do
        cf push "drainspinner-$job_name-$i" -c ./logspinner -b binary_buildpack
        cf bind-service \
            "drainspinner-$job_name-$i" \
            "ss-smoke-syslog-$job_name-drain-$DRAIN_VERSION"
    done
popd
