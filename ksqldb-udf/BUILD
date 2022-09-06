java_library(
    name = "ksqldb_udf",
    srcs = glob(["src/**/*.java"]),
    deps = [
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:com_github_spotbugs_spotbugs_annotations",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest_all",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:org_slf4j_slf4j_reload4j",
        "@maven//:io_confluent_logredactor",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_inline",
        "//ksqldb-test-util:ksqldb_test_util"
    ],
)
