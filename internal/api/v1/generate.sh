#!/bin/bash

dir_resolve()
{
    cd "$1" 2>/dev/null || return $?  # cd to desired directory; if fail, quell any error messages but return exit status
    echo "`pwd -P`" # output full, link-resolved path
}

set -e

TARGET=`dirname $0`
TARGET=`dir_resolve $TARGET`
cd $TARGET

go get github.com/golang/protobuf/{proto,protoc-gen-go}


tmp_dir=$(mktemp -d)
mkdir -p $tmp_dir/scalable-syslog

cp $GOPATH/src/code.cloudfoundry.org/scalable-syslog/internal/api/v1/*proto $tmp_dir/scalable-syslog
cp *.proto $tmp_dir/scalable-syslog

protoc $tmp_dir/scalable-syslog/*.proto --go_out=plugins=grpc:. --proto_path=$tmp_dir/scalable-syslog

rm -r $tmp_dir
