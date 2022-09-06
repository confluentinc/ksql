load("@rules_java//java:defs.bzl", "java_test")

java_library(
    name = "ksqldb_udf",
    srcs = glob(["src/main/**/*.java"]),
    deps = [
        "//ksqldb-test-util:ksqldb_test_util",
        "@maven//:com_github_spotbugs_spotbugs_annotations",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:io_confluent_logredactor",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest_all",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_inline",
        "@maven//:org_slf4j_slf4j_reload4j",
    ],
)

java_library(
    name = "tests",
    testonly = 1,
    srcs = glob(["src/test/**/*.java"]),
    deps = [
        ":ksqldb_udf",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest_all",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_inline",
        "@maven//:org_slf4j_slf4j_reload4j",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:com_github_spotbugs_spotbugs_annotations",
        "//ksqldb-test-util:ksqldb_test_util",
    ],
)

java_test(
   name = "MyTests",
   test_class = "io.confluent.ksql.MyTestsUdf",
   runtime_deps = [":tests"],
)