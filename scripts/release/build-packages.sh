#!/usr/bin/env bash
# Build logic for KSQLDB & its OS packages

set -o pipefail -o nounset -o errexit

function error {
    echo "ERROR: ${*}"
    exit 1
}

WORKSPACE=""
FULL_VERSION=""
UPSTREAM_VERSION=""

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
        *)
            error "Unknown arg ${arg}"
            ;;
    esac
done

[[ -z "${WORKSPACE}" ]] && error "-w/--workspace not specified and is required"
[[ -z "${FULL_VERSION}" ]] && error "-p/--project-version not specified and is required"
[[ -z "${UPSTREAM_VERSION}" ]] && error "-u/--upstream-version not specified and is required"



# This is for two conditions:
# * When we are building a release candidate (when there is a -rcNNN in the FULL_VERSION)
# * When we are build a real release (when there is no -rcNNN and it matches a 3-digit version)
# * All other cases just barf (invalid input/doesn't know how to handle input)
#
# First consider the real release case, we set the release to 1 (as in its ${VERSION}-1)
# next consider the rc case, we set the release to 0.rcNNN (As in its ${VERSION}-0.rcNNN)
#
# We do this so a "real" release is a higher effective "version" (to package managers) than any
# release candidate we generate (1 > 0.rcNNNN), this is then feed into the package builders
# to produce higher versione-d software in the event a testing system would need to "upgrade"
# from an ${VERSION}-0.rcNNN to ${VERSION}-1.
if [[ "${FULL_VERSION}" =~ ^[0-9]+.[0-9]+.[0-9]+-rc[0-9]+$ ]]; then
    RELEASE="0.${FULL_VERSION##*-rc}"
    VERSION="${FULL_VERSION%%-rc*}"
    MAVEN_ARTIFACT_VERSION="${FULL_VERSION}"
elif [[ "${FULL_VERSION}" =~ ^[0-9]+.[0-9]+.[0-9]+$ ]]; then
    RELEASE=1
    VERSION="${FULL_VERSION}"
    FULL_VERSION="${FULL_VERSION}-${RELEASE}"
    MAVEN_ARTIFACT_VERSION="${VERSION}"
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

echo "FULL_VERSION=${FULL_VERSION} VERSION=${VERSION} RELEASE=${RELEASE} MAVEN_ARTIFACT_VERSION=${MAVEN_ARTIFACT_VERSION} UPSTREAM_VERSION=${UPSTREAM_VERSION}"
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
#xmlstarlet ed --inplace -P --update "/_:project/_:parent/_:version" --value "${UPSTREAM_VERSION}" pom.xml
#
## Maven provides some helpfull commands for setting versions/properties
#mvn --batch-mode versions:set          -DgenerateBackupPoms=false "-DnewVersion=${MAVEN_ARTIFACT_VERSION}"
#mvn --batch-mode versions:set-property -DgenerateBackupPoms=false "-DnewVersion=${MAVEN_ARTIFACT_VERSION}" -Dproperty=io.confluent.ksql.version
#mvn --batch-mode versions:set-property -DgenerateBackupPoms=false "-DnewVersion=${UPSTREAM_VERSION}" -Dproperty=io.confluent.schema-registry.version

# Set version for Debian Package
dch --newversion "${FULL_VERSION}" "Release version ${FULL_VERSION}"
dch --release --distribution unstable ""

echo "Versioning Diff:"
git diff | cat

# Commit changes
git add maven-settings.xml
git commit -a -m "build: Setting project version ${MAVEN_ARTIFACT_VERSION} and parent version ${UPSTREAM_VERSION}."

# We run things through fakeroot, which causes issues with finding an .m2/repository location to write. We set the homedir where it does
# have write access too to get around that.
export MAVEN_OPTS="${MAVEN_OPTS-} -Duser.home=/home/jenkins"

# Build Debian Package
git clean -fd
echo "Building Debian Packages"
git-buildpackage -us -uc --git-debian-branch="${work_branch}" --git-upstream-tree="${work_branch}" --git-builder="debuild --preserve-env -d -i -I"

# Build RPM
echo "Building RPM packages"
fakeroot make PACKAGE_TYPE=rpm "VERSION=${MAVEN_ARTIFACT_VERSION}" "RPM_VERSION=${VERSION}" "REVISION=${RELEASE}" -f debian/Makefile rpm

# Build Archive
echo "Building Archive packages"
fakeroot make PACKAGE_TYPE=archive "VERSION=${MAVEN_ARTIFACT_VERSION}" -f debian/Makefile archive

# Collect output
mkdir -p "${WORKSPACE}/output"
cp ../*.{changes,debian.tar.xz,dsc,build,orig.tar.gz,deb} "${WORKSPACE}/output/"
cp ./*.{tar.gz,zip,rpm} "${WORKSPACE}/output/"
echo "Output Contents:"
ls -la "${WORKSPACE}/output/"
