#!/usr/bin/env bash
set -exu

cf login -a api.$CF_SYSTEM_DOMAIN -u $CF_USERNAME -p $CF_PASSWORD -s $CF_SPACE -o $CF_ORG --skip-ssl-validation

pushd ./${DRAIN_TYPE}_drain
    GOOS=linux go build
    app_name=${DRAIN_TYPE}-drain
    cf push $app_name -c ./${DRAIN_TYPE}_drain -b binary_buildpack --no-route
    if [ "$DRAIN_TYPE" == "syslog" ]; then
        cf map-route $app_name tcp.$CF_SYSTEM_DOMAIN --random-port
    else
        cf map-route $app_name $CF_SYSTEM_DOMAIN --hostname $app_name
    fi
    drain_domain=$(cf app $app_name | grep urls | awk '{print $2}')
    cf create-user-provided-service ss-smoke-syslog-${DRAIN_TYPE}-drain-${DRAIN_VERSION} -l "${DRAIN_TYPE}://$drain_domain/drain?drain-version=$DRAIN_VERSION" || true
popd

pushd ../logspinner
    GOOS=linux go build

    for i in `seq 1 $NUM_APPS`; do
        cf push drainspinner-${DRAIN_TYPE}-$i -c ./logspinner -b binary_buildpack
        cf bind-service drainspinner-${DRAIN_TYPE}-$i ss-smoke-syslog-${DRAIN_TYPE}-drain-${DRAIN_VERSION}
    done;
popd
