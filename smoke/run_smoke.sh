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

WORKSPACE=$1
OUTPUT_DIR=$2
DEBS=( $(find $OUTPUT_DIR -name '*.deb' -print) )
echo "Testing deb file ${DEBS[0]}"
run_package_test ${WORKSPACE}/smoke/deb ${DEBS[0]}
