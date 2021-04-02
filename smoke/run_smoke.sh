#! /bin/bash

set -e

WORKSPACE=$1
OUTPUT_DIR=$2
CP_VERSION=$3

run_package_test() {
    local smoke_test_dir
    local package_file
    local tag
    local tmp_dir
    smoke_test_dir=$1
    package_file=$2
    tag=$3
    tmp_dir=$(mktemp -d -t ksqldb-smoke-XXXXXX)
    echo "Created smoke directory for test $tmp_dir"
    cp "$WORKSPACE"/smoke/common/* "$tmp_dir"
    cp "$smoke_test_dir"/* "$tmp_dir"
    cp "$package_file" "$tmp_dir"
    docker build -t "$tag" --build-arg CP_VERSION="$CP_VERSION" "$tmp_dir"
}

DEB=$(find "$OUTPUT_DIR" -name '*.deb' -print | awk 'NR==1')
if [ -z "$DEB" ]; then
  echo "Couldn't find file *.deb in $OUTPUT_DIR"
fi

echo "Testing deb file $DEB"
run_package_test "$WORKSPACE"/smoke/deb "$DEB" ksqldb-package-test-deb
