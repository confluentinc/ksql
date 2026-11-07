#!/usr/bin/env bash
# Unit test for normalize_jar.sh: envelope differences must collapse to the
# same digest; a real class-byte difference must produce a different digest.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
NORM="$DIR/normalize_jar.sh"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/a/com/x" "$TMP/a/META-INF/maven/io.confluent/common-utils" \
         "$TMP/b/com/x" "$TMP/b/META-INF"
printf 'CLASSBYTES' > "$TMP/a/com/x/C.class"
printf 'CLASSBYTES' > "$TMP/b/com/x/C.class"
# Manifests differ only in volatile lines and line order.
printf 'Manifest-Version: 1.0\nBuild-Jdk: 11.0.1\nCreated-By: Apache Maven\n' > "$TMP/a/META-INF/MANIFEST.MF"
printf 'Created-By: Bazel\nManifest-Version: 1.0\nBuild-Jdk: 17.0.9\n'        > "$TMP/b/META-INF/MANIFEST.MF"
# Maven-injected metadata present only in 'a' — must be ignored.
printf 'junk\n' > "$TMP/a/META-INF/maven/io.confluent/common-utils/pom.xml"

( cd "$TMP/a" && zip -q -X -D -r "$TMP/a.jar" . )
( cd "$TMP/b" && zip -q -X -D -r "$TMP/b.jar" . )

HA="$("$NORM" "$TMP/a.jar")"; HB="$("$NORM" "$TMP/b.jar")"
[ "$HA" = "$HB" ] || { echo "FAIL: envelope-only diff produced different digests ($HA vs $HB)"; exit 1; }

# Negative: a real class-byte difference must diverge.
printf 'CLASSBYTEX' > "$TMP/b/com/x/C.class"
rm -f "$TMP/b.jar"; ( cd "$TMP/b" && zip -q -X -D -r "$TMP/b.jar" . )
HB2="$("$NORM" "$TMP/b.jar")"
[ "$HA" != "$HB2" ] || { echo "FAIL: real class-byte diff was not detected"; exit 1; }

echo "PASS"
