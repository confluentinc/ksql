#! /bin/bash

set -e

run_package_test() {
    local smoke_test_dir=$1
    local package_file=$2
    local tmp_dir=$(mktemp -d -t ksqldb-smoke)
    echo "Created smoke directory for test $tmp_dir"
    cp smoke/common/* "$tmp_dir"
    cp "$smoke_test_dir"/* "$tmp_dir"
    cp "$package_file" "$tmp_dir"
    #docker build $tmp_dir
}

run_package_test smoke/deb $1
