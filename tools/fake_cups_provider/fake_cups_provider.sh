#!/bin/bash

set -e

while true
do
    cat $RESPONSE_BODY | openssl s_server \
    -CAfile $CA_CERT \
    -cert   $CERT \
    -key    $KEY \
    -accept $PORT
done
