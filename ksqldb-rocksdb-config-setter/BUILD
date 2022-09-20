load("@rules_java//java:defs.bzl", "java_test")

java_library(
    name = "ksqldb_rocksdb_config_setter",
    srcs = glob([
        "src/main/**/*.java",
    ]),
    visibility = ["//visibility:public"],
    deps = [
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_apache_kafka_kafka_streams",
        "@maven//:org_rocksdb_rocksdbjni",
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
        ":ksqldb_rocksdb_config_setter",
        "@maven//:com_google_guava_guava",
        "@maven//:junit_junit",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_easymock_easymock",
        "@maven//:org_hamcrest_hamcrest_all",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_inline",
        "@maven//:org_rocksdb_rocksdbjni",
    ],
)

java_test(
    name = "MyTests",
    resources = glob(["src/main/resources/**"]),
    test_class = "io.confluent.ksql.MyTestsRocksDbConfigSetter",
    runtime_deps = [":tests"],
)
