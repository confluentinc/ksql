#!/usr/bin/env bash
# Unit test for normalize_jar.sh: envelope differences must collapse to the
# same digest; a real class-byte difference must produce a different digest.
# In --structural mode, debug-info-only differences must collapse, while
# constant values, annotations and MethodParameters must diverge.
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

# --structural: compile real classes. Skipped without a working javac.
JAVAC="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
if ! "$JAVAC" -version >/dev/null 2>&1; then
  echo "SKIP: --structural cases (no working javac; set JAVA_HOME)"
  echo "PASS"; exit 0
fi
# structural_jar <name> <source> [javac flags...]: compile C.java into <name>.jar.
structural_jar() {
  local name="$1" src="$2"; shift 2
  mkdir -p "$TMP/$name/src" "$TMP/$name/out"
  printf '%s\n' "$src" > "$TMP/$name/src/C.java"
  "$JAVAC" --release 17 "$@" -d "$TMP/$name/out" "$TMP/$name/src/C.java"
  ( cd "$TMP/$name/out" && zip -q -X -D -r "$TMP/$name.jar" . )
  "$NORM" --structural "$TMP/$name.jar"
}
BASE='public class C { public static final int X = 1; @Deprecated public void m(int a) { Runnable r = () -> {}; } }'
S_BASE="$(structural_jar base "$BASE" -g)"
# Same source, different constant pool (debug info only): must match.
S_NODEBUG="$(structural_jar nodebug "$BASE" -g:none)"
[ "$S_BASE" = "$S_NODEBUG" ] || { echo "FAIL: --structural: debug-info-only diff produced different digests"; exit 1; }
# Metadata-only differences that `javap -c -p` alone would miss: must diverge.
S_CONST="$(structural_jar const "${BASE/X = 1/X = 2}" -g)"
[ "$S_BASE" != "$S_CONST" ] || { echo "FAIL: --structural: constant value change not detected"; exit 1; }
S_ANNO="$(structural_jar anno "${BASE/@Deprecated /}" -g)"
[ "$S_BASE" != "$S_ANNO" ] || { echo "FAIL: --structural: annotation change not detected"; exit 1; }
S_PARAMS="$(structural_jar params "$BASE" -g -parameters)"
[ "$S_BASE" != "$S_PARAMS" ] || { echo "FAIL: --structural: MethodParameters change not detected"; exit 1; }

echo "PASS"
