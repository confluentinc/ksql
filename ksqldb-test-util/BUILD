load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library", "scala_binary", "scala_test")
load("@rules_java//java:defs.bzl", "java_test")

java_library(
    name = "ksqldb_test_util",
    srcs = glob(["src/main/**/*.java"]),
    deps = [
        "@maven//:com_google_guava_guava",
        "@maven//:com_github_spotbugs_spotbugs_annotations",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest_all",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:org_apache_curator_curator_test",
        "@maven//:org_apache_kafka_kafka_2_13",
        "@maven//:org_apache_kafka_kafka_2_13_test",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_apache_kafka_kafka_clients_test",
        "@maven//:org_easymock_easymock",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_inline",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:org_slf4j_slf4j_reload4j",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:io_confluent_logredactor",
        "@maven//:org_apache_zookeeper_zookeeper",
        "@maven//:org_scala_lang_scala_library",
    ],
    visibility = ["//visibility:public"],
)

java_library(
    name = "tests",
    testonly = 1,
    srcs = glob(["src/test/**/*.java"]),
    deps = [
        ":ksqldb_test_util",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest_all",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_inline",
        "@maven//:org_slf4j_slf4j_reload4j",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:com_github_spotbugs_spotbugs_annotations",
        "@maven//:org_easymock_easymock",
        "@maven//:org_apache_zookeeper_zookeeper",
        "@maven//:com_google_errorprone_error_prone_annotations",
    ],
)

java_test(
   name = "MyTests",
   test_class = "io.confluent.ksql.MyTestsTestUtil",
   runtime_deps = [":tests"],
)