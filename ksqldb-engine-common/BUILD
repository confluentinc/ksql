load("@rules_java//java:defs.bzl", "java_test")

java_library(
    name = "ksqldb_engine_common",
    srcs = glob(["src/main/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//ksqldb-common:ksqldb_common",
        "//ksqldb-test-util:ksqldb_test_util",
        "//ksqldb-udf:ksqldb_udf",
        "@maven//:com_google_guava_guava",
        "@maven//:io_vertx_vertx_core",
        "@maven//:org_apache_kafka_connect_api",
    ],
)

java_library(
    name = "tests",
    testonly = 1,
    srcs = glob(["src/test/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        ":ksqldb_engine_common",
        "//ksqldb-common:ksqldb_common",
        "//ksqldb-test-util:ksqldb_test_util",
        "//ksqldb-udf:ksqldb_udf",
        "@maven//:com_google_guava_guava",
        "@maven//:io_vertx_vertx_core",
        "@maven//:junit_junit",
        "@maven//:org_apache_kafka_connect_api",
        "@maven//:org_hamcrest_hamcrest_all",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_inline",
    ],
)

java_test(
    name = "MyTests",
    test_class = "io.confluent.ksql.MyTestsEngineCommon",
    runtime_deps = [":tests"],
)
