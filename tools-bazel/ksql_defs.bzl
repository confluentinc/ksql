"""Shared ksql test settings, mirroring the root pom's surefire/failsafe config."""

load("@rules_java//java/common:java_info.bzl", "JavaInfo")

# Root pom surefire <argLine>/<systemPropertyVariables> (+ common parent's
# -Djava.awt.headless). log4j2.configurationFile points at ksqldb-test-util's
# config exactly like Maven's file:${project.basedir}/../ksqldb-test-util/...;
# the path is relative to the test cwd (the runfiles root), see KSQL_TEST_DATA.
KSQL_TEST_JVM_FLAGS = [
    "-Duser.timezone=UTC",
    "-Xmx1g",
    "-Xms1g",
    "-Djava.awt.headless=true",
    "-Dlog4j2.configurationFile=ksqldb-test-util/src/main/resources/log4j2.yaml",
    "-Djunit.platform.execution.listeners.deactivate=org.apache.kafka.test.ThreadLeakListener",
]

KSQL_TEST_DATA = ["//ksqldb-test-util:src/main/resources/log4j2.yaml"]

# JUnit platform + engines (JUNIT5_VINTAGE_DEPS) plus the deps every module
# inherits from the root pom's <dependencies> (log4j2 backend, logredactor,
# broker-plugins), so test runtime classpaths match Maven's.
KSQL_TEST_RUNTIME_DEPS = [
    "@maven//:org_junit_jupiter_junit_jupiter_engine",
    "@maven//:org_junit_platform_junit_platform_launcher",
    "@maven//:org_junit_platform_junit_platform_reporting",
    "@maven//:org_junit_vintage_junit_vintage_engine",
] + [
    "@maven//:org_apache_logging_log4j_log4j_core",
    "@maven//:org_apache_logging_log4j_log4j_api",
    "@maven//:org_apache_logging_log4j_log4j_1_2_api",
    "@maven//:io_confluent_confluent_log4j2_extensions",
    "@maven//:io_confluent_logredactor",
    "@maven//:io_confluent_broker_plugins",
]

# kafka-test-common-runtime (DummyLicenseManagerService) and confluent-licensing-new
# (the real one, which creates `_confluent-command`) both register a ServiceLoader
# LicenseManagerService; the first on the classpath wins. Maven's classpath puts
# the dummy first in every module except engine, execution, rest-model and streams.
KSQL_DUMMY_LICENSE_FIRST = ["@maven//:org_apache_kafka_kafka_test_common_runtime"]

# Dependencies every module inherits from the root pom's <dependencies>
# (compile scope; spotbugs-annotations is `provided`) and from its parent
# io.confluent:common (common-utils). A Bazel-generated POM
# has no <parent>, so these are declared on each library directly — that is
# also what Maven's effective POM for each module contains.
KSQL_PARENT_DEPS = [
    "//third_party:spotbugs_annotations-neverlink",
    "@maven//:io_confluent_broker_plugins",
    "@maven//:io_confluent_common_utils",  # io.confluent:common's own <dependencies>
    "@maven//:io_confluent_confluent_log4j2_extensions",
    "@maven//:io_confluent_logredactor",
    "@maven//:org_apache_logging_log4j_log4j_1_2_api",
    "@maven//:org_apache_logging_log4j_log4j_api",
    "@maven//:org_apache_logging_log4j_log4j_core",
]

def _classes_dir_impl(ctx):
    out = ctx.actions.declare_directory(ctx.attr.name)
    jar = ctx.attr.lib[JavaInfo].java_outputs[0].class_jar
    ctx.actions.run_shell(
        inputs = [jar],
        outputs = [out],
        command = "unzip -q -o {} -d {} -x 'META-INF/*'".format(jar.path, out.path),
        mnemonic = "ExplodeClasses",
    )
    return [DefaultInfo(files = depset([out]), runfiles = ctx.runfiles(files = [out]))]

# The module's main classes as a DIRECTORY (Maven's target/classes). ksql's
# test-util ClassFinder (ImmutableTester, *ModelTest, SqlTypesTest, ...) only
# scans classpath *directories* and skips jars; Bazel's classpath is all jars,
# so without this those parameterized tests find zero classes and pass
# vacuously. Tests that need it prepend it via the java stub's
# --main_advice_classpath (see KSQL_CLASSES_DIR_ARGS).
classes_dir = rule(
    implementation = _classes_dir_impl,
    attrs = {"lib": attr.label(providers = [JavaInfo])},
)

def classes_dir_test_args(module):
    """per_test_args entry for a ClassFinder-based test in `module`."""
    return {
        "args": ["--main_advice_classpath=%s/%s-classes" % (module, module)],
        "data": ["//%s:%s-classes" % (module, module)],
    }

def _module_cwd_files_impl(ctx):
    pkg = ctx.label.package + "/"
    symlinks = {}
    for f in ctx.files.srcs:
        rel = f.short_path
        if rel.startswith(pkg):
            rel = rel[len(pkg):]
        symlinks[rel] = f
    root_symlinks = {}
    for tgt, path in ctx.attr.root_symlinks.items():
        for f in tgt.files.to_list():
            root_symlinks[path] = f
    return [DefaultInfo(runfiles = ctx.runfiles(
        symlinks = symlinks,
        root_symlinks = root_symlinks,
    ))]

# Surefire runs tests with cwd = the module directory, so ksql tests open
# files like "src/test/resources/udf-example.jar" or "../ksqldb-engine/...".
# Bazel's test cwd is the runfiles workspace root (<runfiles>/_main). Adding
# this target to a test's `data` mirrors the module's files at
# module-relative paths under _main (so the module dir "is" the cwd), and
# `root_symlinks` covers "../<sibling-module>/..." paths, which resolve to
# <runfiles>/<sibling-module>/... . No test source changes needed.
module_cwd_files = rule(
    implementation = _module_cwd_files_impl,
    attrs = {
        "root_symlinks": attr.label_keyed_string_dict(allow_files = True),
        "srcs": attr.label_list(allow_files = True),
    },
)
