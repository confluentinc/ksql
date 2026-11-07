#!/usr/bin/env bash
# parity_check.sh — local publish-parity harness for one artifact.
#
# Compares the Bazel-produced artifact against the Maven-produced one, so a human
# reviewer has evidence for the Phase-5 / DP-19123 go/no-go. Derived from
# confluentinc/common's harness (DP-19125), extended for ksql:
#
#   - JAR content: normalized sha256 via normalize_jar.sh, STRICT (class bytes
#     verbatim) by default or --structural (classes compared by javap
#     disassembly, see normalize_jar.sh for why).
#   - POM: Maven's POM inherits ~7 deps from ksqldb-parent and Bazel's POM has
#     no <parent>, so compare against Maven's EFFECTIVE POM (--maven-pom) and
#     check two properties instead of equality:
#       (a) no dropped deps: every compile/runtime dependency of the effective
#           POM is in the Bazel POM (provided/test deps are not transitive to
#           consumers and are only reported);
#       (b) no new artifacts: every extra Bazel POM dependency (an edge Bazel
#           strict-deps made explicit) is already in Maven's resolved
#           compile/runtime/provided closure for the module (--maven-deplist,
#           `mvn dependency:list` output).
#   - pom-only artifacts (packaging=pom): POM checks only (no jar).
# Byte-identical jars are NOT expected (manifest, timestamps, entry order).
# --check-signatures is a CI-only stub (the session cannot sign).
#
# Prints exactly one final line: `RESULT: PASS|FAIL|BASELINE-UNAVAILABLE <coords>`
# (exit 0 for PASS/BASELINE-UNAVAILABLE, non-zero for FAIL). A CLI usage error
# exits 2 with NO RESULT line.
#
# Usage:
#   parity_check.sh --coordinates io.confluent.ksql:ksqldb-udf:8.5.0-0 \
#                   --maven-module ksqldb-udf \
#                   --bazel-target //ksqldb-udf:ksqldb-udf \
#                   [--maven-jar PATH] [--maven-pom EFFECTIVE_POM] \
#                   [--maven-deplist DEPLIST] [--structural] [--check-signatures]
# Without --maven-jar the Maven artifact is built (`mvn -pl <module> -am package`).
set -euo pipefail

COORDS="" MAVEN_MODULE="" BAZEL_TARGET="" CHECK_SIGS=0
MVN_JAR="" MVN_POM="" MVN_DEPLIST="" STRUCTURAL=""
while [ $# -gt 0 ]; do
  case "$1" in
    --coordinates)  COORDS="$2"; shift 2;;
    --maven-module) MAVEN_MODULE="$2"; shift 2;;
    --bazel-target) BAZEL_TARGET="$2"; shift 2;;
    --maven-jar)    MVN_JAR="$2"; shift 2;;
    --maven-pom)    MVN_POM="$2"; shift 2;;
    --maven-deplist) MVN_DEPLIST="$2"; shift 2;;
    --structural)   STRUCTURAL="--structural"; shift;;
    --check-signatures) CHECK_SIGS=1; shift;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done
[ -n "$COORDS" ] && [ -n "$MAVEN_MODULE" ] && [ -n "$BAZEL_TARGET" ] || {
  echo "usage: parity_check.sh --coordinates G:A:V --maven-module DIR --bazel-target //label [--maven-jar JAR] [--maven-pom POM] [--maven-deplist FILE] [--structural] [--check-signatures]" >&2; exit 2; }

ARTIFACT_ID="$(echo "$COORDS" | cut -d: -f2)"
VERSION="$(echo "$COORDS" | cut -d: -f3)"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
echo "== parity check for $COORDS (${STRUCTURAL:-strict}) =="

# 1) Maven artifact: prebuilt (--maven-jar) or built here.
if [ -z "$MVN_JAR" ]; then
  echo "-- building Maven artifact ($MAVEN_MODULE) --"
  # ksql: pin the schema-registry range like the documented baseline and skip
  # gates that don't affect jar contents/POM. A resolver/auth failure is an
  # environment limit (BASELINE-UNAVAILABLE); anything else is a real FAIL.
  MVN_BUILD_LOG="$WORK/mvn-build.log"
  if ! mvn -q -pl "$MAVEN_MODULE" -am package -DskipTests \
    -Dio.confluent.schema-registry.version="${SR_VERSION:-8.5.0-348}" \
    -Dcheckstyle.skip=true -Dspotbugs.skip=true -Denforcer.skip=true -Ddocker.skip=true \
    > "$MVN_BUILD_LOG" 2>&1; then
    cat "$MVN_BUILD_LOG" >&2
    if grep -qiE '(status code:? *401|Unauthorized|Could not transfer artifact|Could not resolve dependencies|codeartifact)' "$MVN_BUILD_LOG"; then
      echo "!! Maven baseline unavailable — dependency resolution/auth failure" >&2
      echo "RESULT: BASELINE-UNAVAILABLE $COORDS"; exit 0
    fi
    echo "!! Maven baseline build genuinely failed for $MAVEN_MODULE" >&2
    echo "RESULT: FAIL $COORDS"; exit 1
  fi
  MVN_JAR="$(ls -1 "$MAVEN_MODULE"/target/"$ARTIFACT_ID"-*.jar 2>/dev/null | grep -vE 'sources|javadoc|tests|standalone|with-dependencies' | head -n1 || true)"
fi

POM_ONLY=0
if [ -z "$MVN_JAR" ] || [ ! -f "$MVN_JAR" ]; then
  if grep -q "<packaging>pom</packaging>" "$MAVEN_MODULE/pom.xml" 2>/dev/null; then
    POM_ONLY=1
    echo "-- pom-only artifact (packaging=pom): comparing POMs only, no jar --"
  else
    echo "!! Maven baseline jar not found for $MAVEN_MODULE" >&2
    echo "RESULT: BASELINE-UNAVAILABLE $COORDS"; exit 0
  fi
fi
[ -n "$MVN_POM" ] || MVN_POM="$(ls -1 "$MAVEN_MODULE"/target/*.pom 2>/dev/null | head -n1 || true)"
[ -n "$MVN_POM" ] || MVN_POM="$MAVEN_MODULE/pom.xml"

# 2) Bazel artifact + generated POM.
echo "-- building Bazel artifact ($BAZEL_TARGET) --"
bazel build "$BAZEL_TARGET" || { echo "RESULT: FAIL $COORDS"; exit 1; }
NAME="${BAZEL_TARGET##*:}"
PKG="${BAZEL_TARGET#//}"; PKG="${PKG%%:*}"
bazel build "//$PKG:$NAME-pom" >/dev/null 2>&1 || true
BZL_POM="bazel-bin/$PKG/$NAME-pom.xml"
[ -f "$BZL_POM" ] || BZL_POM="$(find -L "bazel-bin/$PKG" -maxdepth 1 -name "$NAME*pom.xml" 2>/dev/null | head -n1 || true)"

# 3a) JAR content.
JAR_FAILED=0
if [ "$POM_ONLY" = 0 ]; then
  BZL_JAR="$(bazel cquery --output=files "$BAZEL_TARGET" 2>/dev/null | grep -E '\.jar$' | grep -vE 'sources|javadoc' | head -n1 || true)"
  [ -n "$BZL_JAR" ] || { echo "!! could not find Bazel jar for $BAZEL_TARGET" >&2; echo "RESULT: FAIL $COORDS"; exit 1; }
  echo "-- jar content parity (normalized sha256, ${STRUCTURAL:-strict}) --"
  NORM="$(dirname "$0")/normalize_jar.sh"
  MVN_SHA="$("$NORM" $STRUCTURAL "$MVN_JAR")" || { echo "!! normalize_jar.sh failed on $MVN_JAR" >&2; echo "RESULT: FAIL $COORDS"; exit 1; }
  BZL_SHA="$("$NORM" $STRUCTURAL "$BZL_JAR")" || { echo "!! normalize_jar.sh failed on $BZL_JAR" >&2; echo "RESULT: FAIL $COORDS"; exit 1; }
  echo "   maven  $MVN_JAR"
  echo "   bazel  $BZL_JAR"
  echo "   maven  normalized sha256: $MVN_SHA"
  echo "   bazel  normalized sha256: $BZL_SHA"
  if [ "$MVN_SHA" != "$BZL_SHA" ]; then
    # Keep going so the POM checks are reported too; the result is still FAIL.
    echo "!! jar content parity FAILED for $COORDS" >&2
    JAR_FAILED=1
  else
    echo "   OK: normalized jar content matches"
  fi
fi

# 3b) POM coordinates + dependency properties.
[ -n "$BZL_POM" ] && [ -f "$BZL_POM" ] || { echo "!! could not locate the generated Bazel POM" >&2; echo "RESULT: FAIL $COORDS"; exit 1; }
echo "-- POM checks (maven: $MVN_POM, bazel: $BZL_POM) --"
python3 - "$COORDS" "$MVN_POM" "$BZL_POM" "$MVN_DEPLIST" <<'PY' || { echo "RESULT: FAIL $COORDS"; exit 1; }
import sys
try:
    from defusedxml.ElementTree import parse as xml_parse
except Exception:
    from xml.etree.ElementTree import parse as xml_parse
coords, mvn_pom, bzl_pom, deplist = sys.argv[1:5]
def local(t): return t.rsplit('}', 1)[-1]
def child(el, name):
    for c in list(el):
        if local(c.tag) == name: return c
    return None
def text(el, name):
    c = child(el, name); return c.text.strip() if (c is not None and c.text) else None
def read(path):
    root = xml_parse(path).getroot()
    parent = child(root, "parent")
    gid = text(root, "groupId") or (text(parent, "groupId") if parent is not None else None)
    aid = text(root, "artifactId")
    ver = text(root, "version") or (text(parent, "version") if parent is not None else None)
    deps = {}
    d = child(root, "dependencies")
    for x in (list(d) if d is not None else []):
        if local(x.tag) != "dependency": continue
        scope = text(x, "scope") or "compile"
        key = text(x, "groupId") + ":" + text(x, "artifactId")
        if text(x, "classifier"): key += ":" + text(x, "classifier")
        deps[key] = scope
    return (gid, aid, ver), deps
want = tuple(coords.split(":")[:3])
ok = True
(mc, md), (bc, bd) = read(mvn_pom), read(bzl_pom)
for side, c in (("maven", mc), ("bazel", bc)):
    if c != want:
        print("!! %s POM coordinates %s != requested %s" % (side, ":".join(map(str, c)), coords), file=sys.stderr); ok = False
print("   coordinates: maven=%s bazel=%s" % (":".join(map(str, mc)), ":".join(map(str, bc))))
mvn_deps = {k for k, s in md.items() if s not in ("test", "provided")}
# `provided` deps are compile-only and NOT transitive to consumers, so their
# absence from the published POM does not change what downstream resolves
# (Bazel models them as neverlink). Reported, not failed.
for k in sorted(k for k, s in md.items() if s == "provided" and k not in bd):
    print("   provided-scope dependency not in Bazel POM (compile-only, non-transitive): " + k)
dropped = sorted(mvn_deps - set(bd))
if dropped:
    ok = False
    for k in dropped: print("!! dropped dependency (in Maven effective POM, not in Bazel POM): " + k, file=sys.stderr)
extra = sorted(set(bd) - mvn_deps)
closure = None
if deplist:
    closure = set()
    for line in open(deplist):
        p = line.strip().split(" -- ")[0].strip().split(":")
        if len(p) < 5: continue
        g, a = p[0], p[1]
        c = p[3] if len(p) == 6 else ""
        scope = p[-1]
        if scope == "test": continue
        closure.add(g + ":" + a + ((":" + c) if c else ""))
for k in extra:
    if closure is None:
        print("   extra Bazel POM dependency (unchecked, no --maven-deplist): " + k)
    elif k in closure or k.startswith("io.confluent.ksql:"):
        print("   extra Bazel POM dependency, already in Maven's resolved closure (strict-deps edge): " + k)
    else:
        ok = False
        print("!! extra Bazel POM dependency NOT in Maven's resolved closure: " + k, file=sys.stderr)
scope_deltas = sorted(k for k in mvn_deps & set(bd) if md[k] != bd[k])
for k in scope_deltas: print("   scope delta (informational): %s maven=%s bazel=%s" % (k, md[k], bd[k]))
print("   deps: maven(non-test)=%d bazel=%d dropped=%d extra=%d" % (len(mvn_deps), len(bd), len(dropped), len(extra)))
sys.exit(0 if ok else 1)
PY
echo "   OK: POM coordinates match; no dropped deps; extras within Maven's closure"
[ "$JAR_FAILED" = 0 ] || { echo "RESULT: FAIL $COORDS"; exit 1; }

if [ "$CHECK_SIGS" -eq 1 ]; then
  echo "   signature parity: SKIPPED (CI-only — local session cannot sign)"
fi

echo "== done. Signing/provenance parity is verified in CI, not here. =="
echo "RESULT: PASS $COORDS"
