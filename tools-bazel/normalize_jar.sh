#!/usr/bin/env bash
# normalize_jar.sh — emit a content sha256 for one jar, independent of build
# envelope. Two modes:
#
#   normalize_jar.sh <jar>               STRICT: class/resource BYTES verbatim
#   normalize_jar.sh --structural <jar>  STRUCTURAL: .class files compared by
#                                        their `javap -c -p` disassembly
#
# Envelope normalized away in both modes:
#   - META-INF/maven/**            dropped (Maven-injected build metadata)
#   - git.properties               dropped (git-commit-id-plugin; Maven-only)
#   - MANIFEST.MF volatile lines   stripped, remaining lines sorted
#   - zip timestamps / entry order irrelevant (we hash extracted file contents)
#
# STRUCTURAL exists because Bazel's javac (java_tools, newer than JDK 17) and
# Maven's JDK 17 javac emit semantically identical but byte-different class
# files (constant-pool order, synthetic lambda numbering). It compares the
# disassembled members + bytecode with lambda$<m>$<n> indices normalized;
# resources are still compared byte-for-byte. Needs `javap` (JAVAP, JAVA_HOME
# or PATH).
set -euo pipefail
MODE=strict
if [ "${1:-}" = "--structural" ]; then MODE=structural; shift; fi
JAR="$1"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT

# Portable sha256: GNU coreutils `sha256sum` (Linux/CI) or `shasum -a 256`
# (a stock macOS ships shasum, not sha256sum). Both print "<hash>  <name>".
_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$@"
  else
    shasum -a 256 "$@"
  fi
}
unzip -q -o "$JAR" -d "$WORK"

# Drop Maven-injected metadata.
rm -rf "$WORK/META-INF/maven" "$WORK/git.properties"

# Normalize the manifest: strip volatile lines, sort the remainder.
MF="$WORK/META-INF/MANIFEST.MF"
if [ -f "$MF" ]; then
  grep -avE '^(Build-Jdk|Build-Jdk-Spec|Created-By|Built-By|Bnd-LastModified|Tool|Archiver-Version|Commit-ID|Java-Version|Target-Label|Injecting-Rule-Kind):' "$MF" \
    | sed '/^[[:space:]]*$/d' | LC_ALL=C sort > "$MF.norm" || true
  mv "$MF.norm" "$MF"
  # A manifest left with only "Manifest-Version" carries no content.
  if ! grep -qv '^Manifest-Version:' "$MF"; then rm -f "$MF"; fi
fi

if [ "$MODE" = structural ]; then
  JAVAP="${JAVAP:-}"
  if [ -z "$JAVAP" ]; then
    if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/javap" ]; then JAVAP="$JAVA_HOME/bin/javap"; else JAVAP="$(command -v javap)"; fi
  fi
  # Replace all .class files with ONE normalized disassembly, in sorted path
  # order (javap prints its arguments' output in order; batched to bound JVM
  # start-ups). Paths stay in the listing so a moved class still diverges.
  ( cd "$WORK" && find . -name '*.class' | LC_ALL=C sort ) > "$WORK.classes"
  if [ -s "$WORK.classes" ]; then
    ( cd "$WORK" && tr '\n' '\0' < "$WORK.classes" | xargs -0 -n 500 "$JAVAP" -c -p ) \
      | sed -E 's/lambda\$([A-Za-z0-9_]+)\$[0-9]+/lambda$\1$N/g' > "$WORK/classes.javap"
    cp "$WORK.classes" "$WORK/classes.list"
    ( cd "$WORK" && tr '\n' '\0' < "$WORK.classes" | xargs -0 rm -f )
  fi
  rm -f "$WORK.classes"
fi

# Digest: "relpath  sha256(content)" for every file, sorted, then hash the list.
( cd "$WORK" && find . -type f | LC_ALL=C sort | while read -r f; do
    printf '%s  %s\n' "${f#./}" "$(_sha256 "$f" | cut -d' ' -f1)"
  done ) | _sha256 | cut -d' ' -f1
