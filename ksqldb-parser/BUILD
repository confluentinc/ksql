load("@rules_java//java:defs.bzl", "java_test")
load("@rules_antlr//antlr:antlr4.bzl", "antlr")

package(default_visibility = ["//visibility:public"])

antlr(
    name = "parser",
    srcs = ["src/main/antlr4/io/confluent/ksql/parser/SqlBase.g4"],
    package = "io.confluent.ksql.parser",
    visitor = True,
)

java_library(
    name = "generated",
    srcs = [":parser"],
    deps = ["@maven//:org_antlr_antlr4_runtime"],
)

java_library(
    name = "ksqldb_parser",
    srcs = glob([
        "src/main/**/*.java",
        ":generated",
    ]),
    visibility = ["//visibility:public"],
    deps = [
        ":generated",
        "//ksqldb-common:ksqldb_common",
        "//ksqldb-execution:ksqldb_execution",
        "//ksqldb-metastore:ksqldb_metastore",
        "//ksqldb-serde:ksqldb_serde",
        "//ksqldb-udf:ksqldb_udf",
        "@maven//:com_approvaltests_approvaltests",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
        "@maven//:com_fasterxml_jackson_datatype_jackson_datatype_jsr310",
        "@maven//:com_github_spotbugs_spotbugs_annotations",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_airlift_slice",
        "@maven//:org_antlr_antlr4_runtime",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_commons_commons_text",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_apache_kafka_kafka_streams",
    ],
)

java_library(
    name = "tests",
    testonly = 1,
    srcs = glob(["src/test/**/*.java"]),
    deps = [
        ":generated",
        ":ksqldb_parser",
        "//ksqldb-common:ksqldb_common",
        "//ksqldb-execution:ksqldb_execution",
        "//ksqldb-metastore:ksqldb_metastore",
        "//ksqldb-metastore:tests",
        "//ksqldb-serde:ksqldb_serde",
        "//ksqldb-test-util:ksqldb_test_util",
        "//ksqldb-udf:ksqldb_udf",
        "@maven//:com_approvaltests_approvaltests",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:io_airlift_slice",
        "@maven//:junit_junit",
        "@maven//:org_antlr_antlr4_runtime",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_commons_commons_text",
        "@maven//:org_apache_kafka_connect_api",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_apache_kafka_kafka_streams",
        "@maven//:org_easymock_easymock",
        "@maven//:org_hamcrest_hamcrest_all",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_inline",
    ],
)

java_test(
    name = "MyTests",
    test_class = "io.confluent.ksql.MyTestsParser",
    runtime_deps = [":tests"],
)
