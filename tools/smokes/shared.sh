
# JOB_NAME:
# DRAIN_TYPE:
# DRAIN_VERSION:
# CF_SYSTEM_DOMAIN:
# CF_USERNAME:
# CF_PASSWORD:
# CF_SPACE:
# CF_ORG:

function login {
    checkpoint "Logging into CF"

    if [ "${login_has_occurred:-}" = "" ]; then
        cf login \
            -a api."$CF_SYSTEM_DOMAIN" \
            -u "$CF_USERNAME" \
            -p "$CF_PASSWORD" \
            -s "$CF_SPACE" \
            -o "$CF_ORG" \
            --skip-ssl-validation # TODO: consider passing this in as a param
    fi
    login_has_occurred=true
}

function checkpoint {
    echo
    echo -e "\e[95m##### $1 #####\e[0m"
}

function error {
    echo -e "\e[91m$1\e[0m"
}

function warning {
    echo -e "\e[93m$1\e[0m"
}

function success {
    echo -e "\e[92m$1\e[0m"
}

function app_url {
    local app_name="$1"

    if [ "$app_name" = "" ]; then
        echo app name not provided
        exit 22
    fi

    local guid=$(cf app "$app_name" --guid)
    local route_data=$(cf curl "/v2/apps/$guid/routes")
    local domain_url=$(echo "$route_data" | jq .resources[0].entity.domain_url --raw-output)
    local domain_name=$(cf curl "$domain_url" | jq .entity.name --raw-output)

    local port=$(echo "$route_data" | jq .resources[0].entity.port --raw-output)
    if [ "$port" != "null" ]; then
        # this app uses tcp routing
        echo "$domain_name:$port"
    else
        local host=$(echo "$route_data" | jq .resources[0].entity.host --raw-output)
        echo "$host.$domain_name"
    fi
}

function job_name {
    echo "${JOB_NAME:-$DRAIN_TYPE}"
}

function drain_app_name {
    echo "drain-$(job_name)"
}

function drainspinner_app_name {
    echo "drainspinner-$(job_name)"
}

function counter_app_name {
    echo "counter-$(job_name)"
}

function syslog_drain_service_name {
    echo "ss-smoke-syslog-$(job_name)-drain-$DRAIN_VERSION"
}

function syslog_drain_service_url {
    echo "$DRAIN_TYPE://$(app_url $(drain_app_name))/drain?drain-version=$DRAIN_VERSION"
}
