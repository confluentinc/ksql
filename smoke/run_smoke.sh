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
    docker build $tmp_dir
}

WORKING_DIR=$1
echo "Testing deb file $2"
run_package_test ${WORKING_DIR}/smoke/deb $2
