# Local Java build wrappers for a Confluent Bazel repo.
#
# Copy this to `tools-bazel/library_wrappers.bzl` in the target repo (drop the
# `.tmpl` suffix) and load its symbols from your BUILD files instead of the raw
# rules. Distilled from ce-kafka's tools-bazel/library_wrappers.bzl — Java only.
#
# Why wrappers? They give every module the same org defaults in one place:
#   - a single POM template for published artifacts,
#   - a `-neverlink` twin for compile-only ("provided"/Gradle compileOnly) deps,
#   - the `excluded_workspaces` set so tool jars never leak into published jars,
#   - the confluent_rules JUnit5 test-suite macro under a stable local name.
#
# Loads assume: bazel_dep on rules_java, rules_jvm_external, contrib_rules_jvm,
# and the confluent_rules archive_override (see MODULE.bazel from Phase 1).

load("@confluent_rules//java:java_junit_test_suite.bzl", "java_junit_test_suite")
load("@contrib_rules_jvm//java:defs.bzl", "java_junit5_test", oss_java_export = "java_export")
load("@rules_java//java:defs.bzl", oss_java_library = "java_library")
load("@rules_jvm_external//:defs.bzl", oss_maven_export = "maven_export")

# Tool/proto/grpc workspaces whose jars must never end up inside a published
# artifact. Extend per repo as strict-deps/packaging surfaces more.
_JAR_PKG_EXCLUDED_WORKSPACES = {
    "protobuf": None,
    "com_google_protobuf": None,
    "contrib_rules_jvm_deps": None,
}

# Default POM template used for every published artifact. Copy assets/pom.tpl to
# //tools-bazel:pom.tpl. Preserves io.confluent coordinates at $(maven_version).
_POM_TEMPLATE = "//tools-bazel:pom.tpl"

def java_library(name, neverlink_option = False, **attrs):
    """java_library with an optional compile-only (`-neverlink`) twin.

    Set neverlink_option = True to also emit `<name>-neverlink` (Gradle
    `compileOnly` / Maven `provided` equivalent): compiled against, not shipped.
    """
    oss_java_library(name = name, **attrs)

    if neverlink_option:
        oss_java_library(
            name = name + "-neverlink",
            neverlink = True,
            tags = ["maven:compile-only"],
            exports = [name],
            visibility = attrs.get("visibility", ["//visibility:private"]),
        )

def java_export(name, pom_template = _POM_TEMPLATE, exclusions = {}, **attrs):
    """Publishable library. Wraps contrib_rules_jvm java_export.

    Generates `<name>.publish` (a maven_publish target) automatically. Pass
    `maven_coordinates = "io.confluent:<artifact>:$(maven_version)"` so the CP
    nano-version flows into the coordinates (see Phase 5). Prefer this for any
    module that other repos consume from CodeArtifact.
    """
    oss_java_export(
        name = name,
        pom_template = pom_template,
        exclusions = exclusions,
        excluded_workspaces = _JAR_PKG_EXCLUDED_WORKSPACES,
        **attrs
    )

def maven_export(name, lib_name, pom_template = _POM_TEMPLATE, exclusions = {}, **attrs):
    """Attach publishing to an existing java_library (the cc-unified-storage
    pattern). Use when a module must stay a plain java_library that other
    in-repo targets depend on, and you add publishing separately.

    The java_library carries `tags = ["maven_coordinates=io.confluent:<art>:$(maven_version)"]`;
    this target references it via `lib_name`. Other maven_exports that depend on
    this artifact must depend on THIS target (not the java_library) to get a
    correctly structured POM.
    """
    oss_maven_export(
        name = name,
        lib_name = lib_name,
        pom_template = pom_template,
        exclusions = exclusions,
        excluded_workspaces = _JAR_PKG_EXCLUDED_WORKSPACES,
        **attrs
    )

def jvm_junit_test_suite(**attrs):
    """JUnit5 test suite: one java_junit5_test per `*Test.java`, plus a root
    test_suite. Thin alias over confluent_rules so BUILD files use a stable
    local name. See Phase 4 for required runtime_deps.
    """
    java_junit_test_suite(**attrs)

def _fqcn(src, package_root):
    """"src/test/java/io/confluent/foo/BarTest.java" + root "src/test/java"
    -> "io.confluent.foo.BarTest"."""
    marker = package_root.rstrip("/") + "/"
    idx = src.find(marker)
    rel = src[idx + len(marker):] if idx >= 0 else src
    return rel[:-len(".java")].replace("/", ".")

def jvm_junit_test_classes(
        name,
        test_lib,
        test_srcs,
        runtime_deps = [],
        classpath_first = [],
        package_root = "src/test/java",
        integration_srcs = [],
        per_test_args = {},
        jvm_flags = [],
        data = [],
        size = "medium",
        tags = [],
        visibility = None,
        **kwargs):
    """One java_junit5_test per test class, all sharing ONE compiled test lib.

    ksql-specific variant of jvm_junit_test_suite: the confluent_rules macro
    compiles every `*Test.java` as its own javac action, which (a) breaks when
    test classes reference each other (common in ksql) and (b) means ~1,170
    javac runs over a ~600-jar classpath. Here the module's whole src/test tree
    compiles once into `test_lib` (which also stands in for the Maven
    `tests` test-jar other modules depend on), and each test class gets a
    java_junit5_test that only selects `test_class`.

    `integration_srcs` (JUnit4 @Category(IntegrationTest) classes — run by
    failsafe under Maven) are tagged `integration`. `per_test_args` is keyed
    by source path like the confluent_rules macro (tags/size/jvm_flags/...).
    `classpath_first` jars go ahead of `test_lib` on the runtime classpath.
    """
    simple_counts = {}
    for src in test_srcs:
        simple = src[src.rfind("/") + 1:-len(".java")]
        simple_counts[simple] = simple_counts.get(simple, 0) + 1

    tests = []
    for src in test_srcs:
        args = dict(per_test_args.get(src, {}))
        t_tags = tags + args.pop("tags", [])
        is_integration = src in integration_srcs
        if is_integration and "integration" not in t_tags:
            t_tags = t_tags + ["integration"]
        t_flags = jvm_flags + args.pop("jvm_flags", [])

        # Root pom surefire: rerunFailingTestsCount=3 (failsafe has none).
        if not is_integration:
            args.setdefault("flaky", True)

        # Embedded-Kafka integration tests routinely exceed `medium` (300s)
        # when run in parallel; failsafe allows a 3600s fork timeout.
        t_size = args.pop("size", "large" if is_integration else size)
        t_data = data + args.pop("data", [])
        fqcn = _fqcn(src, package_root)
        simple = src[src.rfind("/") + 1:-len(".java")]

        # Simple class name unless two classes share it (then the FQCN).
        tname = simple if simple_counts[simple] == 1 else fqcn.replace(".", "_")
        java_junit5_test(
            name = tname,
            test_class = fqcn,
            runtime_deps = classpath_first + [test_lib] + runtime_deps,
            jvm_flags = t_flags,
            data = t_data,
            size = t_size,
            tags = t_tags,
            visibility = visibility,
            **dict(kwargs, **args)
        )
        tests.append(":" + tname)
    native.test_suite(name = name, tests = tests, tags = tags, visibility = visibility)
