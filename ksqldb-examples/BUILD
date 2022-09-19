load("@rules_java//java:defs.bzl", "java_test")

java_library(
    name = "ksqldb_examples",
    srcs = glob([
        "src/main/**/*.java",
    ]),
    visibility = ["//visibility:public"],
    deps = [
        "//ksqldb-common:ksqldb_common",
        "//ksqldb-engine:ksqldb_engine",
        "//ksqldb-serde:ksqldb_serde",
        "@maven//:com_google_guava_guava",
        "@maven//:io_confluent_avro_avro_random_generator",
        "@maven//:io_confluent_kafka_connect_avro_data",
        "@maven//:io_confluent_kafka_json_schema_provider",
        "@maven//:io_confluent_kafka_protobuf_provider",
        "@maven//:io_confluent_kafka_schema_registry_client",
        "@maven//:org_apache_avro_avro",
        "@maven//:org_apache_kafka_connect_api",
        "@maven//:org_apache_kafka_connect_runtime",
        "@maven//:org_apache_kafka_kafka_clients",
    ],
)

java_library(
    name = "tests",
    testonly = 1,
    srcs = glob(["src/test/**/*.java"]),
    resources = glob([
        "src/test/resources/**",
        "src/main/resources/**",
    ]),
    deps = [
        ":ksqldb_examples",
        "//ksqldb-common:ksqldb_common",
        "//ksqldb-common:tests",
        "//ksqldb-test-util:ksqldb_test_util",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_google_guava_guava",
        "@maven//:io_confluent_avro_avro_random_generator",
        "@maven//:io_confluent_common_utils",
        "@maven//:junit_junit",
        "@maven//:org_apache_avro_avro",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_kafka_connect_api",
        "@maven//:org_apache_kafka_connect_json",
        "@maven//:org_apache_kafka_connect_runtime",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_easymock_easymock",
        "@maven//:org_hamcrest_hamcrest_all",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_inline",
    ],
)

java_test(
    name = "MyTests",
    resources = glob([
        "src/main/resources/**",
        "src/test/resources/**",
    ]),
    test_class = "io.confluent.ksql.MyTestsExamples",
    runtime_deps = [":tests"],
)
