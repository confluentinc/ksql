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
  exit 1
fi
RPM=$(find "$OUTPUT_DIR" -name '*.rpm' -print | awk 'NR==1')
if [ -z "$RPM" ]; then
  echo "Couldn't find file *.rpm in $OUTPUT_DIR"
  exit 1
fi
TGZ=$(find "$OUTPUT_DIR" -name '*.tar.gz' -print | grep -v orig | awk 'NR==1')
if [ -z "$TGZ" ]; then
  echo "Couldn't find file *.tar.gz in $OUTPUT_DIR"
  exit 1
fi

echo "Testing deb file $DEB"
run_package_test "$WORKSPACE"/smoke/deb "$DEB" ksqldb-package-test-deb

echo "Testing rpm file $RPM"
run_package_test "$WORKSPACE"/smoke/rpm "$RPM" ksqldb-package-test-rpm

echo "Testing tgz file $TGZ"
run_package_test "$WORKSPACE"/smoke/tgz "$TGZ" ksqldb-package-test-tgz