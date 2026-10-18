#!/usr/bin/env bash
# Workspace-status script for stamped builds. Wire it in .bazelrc with:
#   build:release --stamp --workspace_status_command=tools-bazel/stamp.sh
# Copy to tools-bazel/stamp.sh in the target repo and `chmod +x` it.
#
# IMPORTANT (CP nano-versioning): the Maven artifact VERSION is NOT derived
# here. It comes from `--define maven_version=<X.Y.Z-N-N-...>` supplied by
# .bazelrc.release-version (see Phase 5). Do not reintroduce a git-describe
# semver here for the maven coordinate — that would break the downstream
# nano-version contract. The keys below feed image labels / build metadata only.
#
# Keys prefixed with STABLE_ bust the Bazel cache when they change; unprefixed
# keys (e.g. TIMESTAMP) are volatile and do not trigger rebuilds.
set -euo pipefail

echo "STABLE_GIT_COMMIT $(git rev-parse HEAD 2>/dev/null || echo unknown)"
echo "STABLE_GIT_BRANCH $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
echo "STABLE_SEMAPHORE_JOB_ID ${SEMAPHORE_JOB_ID:-none}"
echo "STABLE_VERSION ${IMAGE_VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo 0.0.0-dev)}"
echo "TIMESTAMP $(date -u +%Y-%m-%dT%H:%M:%SZ)"
