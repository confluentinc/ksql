load("@rules_java//java:defs.bzl", "java_test")

java_library(
    name = "ksqldb_benchmark",
    srcs = glob([
        "src/main/**/*.java",
    ]),
    visibility = ["//visibility:public"],
    deps = [
        "//ksqldb-common:ksqldb_common",
        "//ksqldb-engine:ksqldb_engine",
        "//ksqldb-examples:ksqldb_examples",
        "//ksqldb-serde:ksqldb_serde",
        "@maven//:com_github_spotbugs_spotbugs_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_confluent_avro_avro_random_generator",
        "@maven//:io_confluent_kafka_connect_avro_data",
        "@maven//:io_confluent_kafka_schema_registry_client",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_openjdk_jmh_jmh_core",
        "@maven//:org_openjdk_jmh_jmh_generator_annprocess",
    ],
)

java_library(
    name = "tests",
    testonly = 1,
    srcs = glob(
        ["src/test/**/*.java"],
        exclude = ["src/test/resources/**"],
    ),
    resources = glob(["src/main/resources/**"]),
    visibility = ["//visibility:public"],
    deps = [
        ":ksqldb_benchmark",
        "@maven//:com_google_guava_guava",
        "@maven//:junit_junit",
        "@maven//:org_easymock_easymock",
        "@maven//:org_hamcrest_hamcrest_all",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_inline",
        "@maven//:org_openjdk_jmh_jmh_core",
    ],
)

java_test(
    name = "MyTests",
    resources = glob(["src/main/resources/**"]),
    test_class = "io.confluent.ksql.MyTestsBenchmark",
    runtime_deps = [":tests"],
)
