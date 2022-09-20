load("@rules_java//java:defs.bzl", "java_test")

java_library(
    name = "tests",
    testonly = 1,
    srcs = glob(
        ["src/test/**/*.java"],
    ),
    visibility = ["//visibility:public"],
    deps = [
        "//ksqldb-common:ksqldb_common",
        "//ksqldb-engine:ksqldb_engine",
        "//ksqldb-rest-app:ksqldb_rest_app",
        "@maven//:com_google_guava_guava",
        "@maven//:io_vertx_vertx_core",
        "@maven//:junit_junit",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_easymock_easymock",
        "@maven//:org_hamcrest_hamcrest_all",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_inline",
        "@maven//:org_reactivestreams_reactive_streams",
        "@maven//:org_reactivestreams_reactive_streams_tck",
        "@maven//:org_rocksdb_rocksdbjni",
        "@maven//:org_testng_testng",
    ],
)

# todo these tests run using testng. I'm sure its possible to run testng, but not spending the time
# on it during the poc.
#java_test(
#    name = "MyTests",
#    test_class = "io.confluent.ksql.MyTestsApiReactiveStreamsTck",
#    runtime_deps = [":tests"],
#)
