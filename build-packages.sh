#!/usr/bin/env bash

set -o pipefail -o nounset -o errexit

function error {
    echo "ERROR: ${*}"
    exit 1
}

WORKSPACE=""
FULL_VERSION=""
UPSTREAM_VERSION=""
BUILD_JAR="false"
while [[ "${#}" -gt 0 ]]; do
    arg="${1}"
    case "${arg}" in
	    -w|--workspace)
            WORKSPACE="${2}"
            [ -d "${WORKSPACE}" ] || error "'${WORKSPACE}' Doesn't exist or is not a directory"
            shift 2
            ;;
        -p|--project-version)
            FULL_VERSION="${2}"  
            shift 2
            ;;
        -u|--upstream-version)
            UPSTREAM_VERSION="${2}"
            shift 2
            ;;
        -j|--jar)
            BUILD_JAR="true"
            shift
            ;;
        *)
            error "Unknown arg ${arg}"
            ;;
    esac
done

[[ -z "${WORKSPACE}" ]] && error "-w/--workspace not specified and is required"
[[ -z "${FULL_VERSION}" ]] && error "-p/--project-version not specified and is required"
[[ -z "${UPSTREAM_VERSION}" ]] && error "-u/--upstream-version not specified and is required"
 

if [[ "${FULL_VERSION}" =~ ^[0-9]+.[0-9]+.[0-9]+-rc[0-9]+$ ]]; then
    RELEASE="0.${FULL_VERSION##*-rc}"
    VERSION="${FULL_VERSION%%-rc*}"
elif [[ "${FULL_VERSION}" =~ ^[0-9]+.[0-9]+.[0-9]+$ ]]; then
    RELEASE=1
    VERSION="${FULL_VERSION}"
    FULL_VERSION="${FULL_VERSION}-${RELEASE}"
else
    error "Unknown version format '${FULL_VERSION}' "
fi

REQUIRED_UTILS=(
    mvn
    make
    tar
    zip
    rpmbuild
    debuild
    git-buildpackage
    xmlstarlet
)

for cmd in "${REQUIRED_UTILS[@]}"; do
    if ! command -v "${cmd}" >/dev/null; then
        echo "Required Utility '${cmd}' not found"
        exit 1
    fi
done

echo "FULL_VERSION=${FULL_VERSION} VERSION=${VERSION} REELASE=${RELEASE} UPSTREAM_VERSION=${UPSTREAM_VERSION}"
cd "${WORKSPACE}"
work_branch="debian-${FULL_VERSION}"
git checkout -b "${work_branch}"
export DEBEMAIL="Confluent Packaging <packages@confluent.io>"


# Set version (Final or RC)

# Maven does not provide a way to set the parent project version at the root pom, so we use
# a tool called xmlstarlet to edit it.
# ed       = Edit
# --inpace = Edit in place
# -P       = Perserve Whitespace nodes (will not preserve attribute node whitespace)
# --update = Update this XPATH Node
# --value  = The value in which to place things.
# The XPATH requires the lambda (_) namespace specified to get to /project/parent/version
xmlstarlet ed --inplace -P --update "/_:project/_:parent/_:version" --value "${UPSTREAM_VERSION}" pom.xml

# Maven provides some helpfull commands for setting versions/properties
mvn --batch-mode versions:set          -DgenerateBackupPoms=false "-DnewVersion=${FULL_VERSION}"
mvn --batch-mode versions:set-property -DgenerateBackupPoms=false "-DnewVersion=${FULL_VERSION}" -Dproperty=io.confluent.ksql.version 
mvn --batch-mode versions:set-property -DgenerateBackupPoms=false "-DnewVersion=${UPSTREAM_VERSION}" -Dproperty=io.confluent.schema-registry.version

# Set version for Debian Package
dch --newversion "${FULL_VERSION}" "Release version ${FULL_VERSION}"
dch --release --distribution unstable ""

# Commit changes
git commit -a -m "build: Setting project version ${FULL_VERSION} and parent version ${UPSTREAM_VERSION}."

if "${BUILD_JAR}"; then
    # Build Jar
    mvn --batch-mode -Pjenkins clean package dependency:analyze site validate -U \
        "-DskipTests" \
        "-Dspotbugs.skip" \
        "-Dcheckstyle.skip" \
        "-Ddocker.tag=${FULL_VERSION}" \
        "-Ddocker.registry=368821881613.dkr.ecr.us-west-2.amazonaws.com/" \
        "-Ddocker.upstream-tag=${UPSTREAM_VERSION}-latest" \
        "-Dskip.docker.build=false"
fi 
# Build Debian Package
git clean -fd 
git-buildpackage -us -uc --git-debian-branch="${work_branch}" --git-upstream-tree="${work_branch}" --git-builder="debuild --set-envvar=VERSION=${FULL_VERSION} -d -i -I"

# Build RPM
make PACKAGE_TYPE=rpm "VERSION=${FULL_VERSION}" "RPM_VERSION=${VERSION}" "REVISION=${RELEASE}" -f debian/Makefile rpm

# Build Archive
make PACKAGE_TYPE=archive "VERSION=${FULL_VERSION}" -f debian/Makefile archive

# Collect output
mkdir -p "${WORKSPACE}/output"
cp ../*.{changes,debian.tar.xz,dsc,build,orig.tar.gz} "${WORKSPACE}/output/"
cp ./*.{tar.gz,zip,rpm} "${WORKSPACE}/output/"
echo "Output Contents:"
ls -la "${WORKSPACE}/output/"
