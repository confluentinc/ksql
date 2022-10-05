load("@rules_java//java:defs.bzl", "java_test")

java_library(
    name = "ksqldb_engine",
    srcs = glob([
        "src/main/**/*.java",
    ]),
    visibility = ["//visibility:public"],
    deps = [
        "//ksqldb-common:ksqldb_common",
        "//ksqldb-engine-common:ksqldb_engine_common",
        "//ksqldb-execution:ksqldb_execution",
        "//ksqldb-metastore:ksqldb_metastore",
        "//ksqldb-parser:generated",
        "//ksqldb-parser:ksqldb_parser",
        "//ksqldb-rest-model:ksqldb_rest_model",
        "//ksqldb-serde:ksqldb_serde",
        "//ksqldb-streams:ksqldb_streams",
        "//ksqldb-udf:ksqldb_udf",
        "@maven//:ch_qos_reload4j_reload4j",
        "@maven//:com_approvaltests_approvaltests",
        "@maven//:com_approvaltests_approvaltests_util",
        "@maven//:com_clearspring_analytics_stream",
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
        "@maven//:com_fasterxml_jackson_datatype_jackson_datatype_jsr310",
        "@maven//:com_github_rholder_guava_retrying",
        "@maven//:com_github_spotbugs_spotbugs_annotations",
        "@maven//:com_github_tomakehurst_wiremock_jre8",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:com_squareup_javapoet",
        "@maven//:commons_codec_commons_codec",
        "@maven//:io_airlift_slice",
        "@maven//:io_confluent_common_utils",
        "@maven//:io_confluent_kafka_json_schema_provider",
        "@maven//:io_confluent_kafka_protobuf_provider",
        "@maven//:io_confluent_kafka_protobuf_serializer",
        "@maven//:io_confluent_kafka_schema_registry_client",
        "@maven//:io_confluent_kafka_schema_serializer",
        "@maven//:io_github_classgraph_classgraph",
        "@maven//:io_vertx_vertx_core",
        "@maven//:org_antlr_antlr4_runtime",
        "@maven//:org_apache_avro_avro",
        "@maven//:org_apache_commons_commons_compress",
        "@maven//:org_apache_commons_commons_csv",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_commons_commons_text",
        "@maven//:org_apache_httpcomponents_client5_httpclient5",
        "@maven//:org_apache_httpcomponents_client5_httpclient5_fluent",
        "@maven//:org_apache_httpcomponents_core5_httpcore5",
        "@maven//:org_apache_kafka_connect_api",
        "@maven//:org_apache_kafka_connect_runtime",
        "@maven//:org_apache_kafka_connect_transforms",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_apache_kafka_kafka_streams",
        "@maven//:org_codehaus_janino_janino",
        "@maven//:org_reactivestreams_reactive_streams",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:org_slf4j_slf4j_reload4j",
    ],
)

java_library(
    name = "tests",
    testonly = 1,
    srcs = glob(
        ["src/test/**/*.java"],
        exclude = ["src/test/resources/**"],
    ),
    visibility = ["//visibility:public"],
    deps = [
        ":ksqldb_engine",
        "//ksqldb-common:ksqldb_common",
        "//ksqldb-common:tests",
        "//ksqldb-engine-common:ksqldb_engine_common",
        "//ksqldb-execution:ksqldb_execution",
        "//ksqldb-metastore:ksqldb_metastore",
        "//ksqldb-metastore:tests",
        "//ksqldb-parser:generated",
        "//ksqldb-parser:ksqldb_parser",
        "//ksqldb-rest-model:ksqldb_rest_model",
        "//ksqldb-serde:ksqldb_serde",
        "//ksqldb-streams:ksqldb_streams",
        "//ksqldb-test-util:ksqldb_test_util",
        "//ksqldb-udf:ksqldb_udf",
        "@maven//:ch_qos_reload4j_reload4j",
        "@maven//:com_approvaltests_approvaltests",
        "@maven//:com_approvaltests_approvaltests_util",
        "@maven//:com_clearspring_analytics_stream",
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
        "@maven//:com_fasterxml_jackson_datatype_jackson_datatype_jsr310",
        "@maven//:com_github_rholder_guava_retrying",
        "@maven//:com_github_spotbugs_spotbugs_annotations",
        "@maven//:com_github_tomakehurst_wiremock_jre8",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:com_squareup_javapoet",
        "@maven//:commons_codec_commons_codec",
        "@maven//:io_airlift_slice",
        "@maven//:io_confluent_common_utils",
        "@maven//:io_confluent_confluent_log4j_extensions",
        "@maven//:io_confluent_kafka_connect_avro_data",
        "@maven//:io_confluent_kafka_json_schema_provider",
        "@maven//:io_confluent_kafka_protobuf_provider",
        "@maven//:io_confluent_kafka_protobuf_serializer",
        "@maven//:io_confluent_kafka_schema_registry_client",
        "@maven//:io_confluent_kafka_schema_serializer",
        "@maven//:io_github_classgraph_classgraph",
        "@maven//:io_vertx_vertx_core",
        "@maven//:junit_junit",
        "@maven//:org_antlr_antlr4_runtime",
        "@maven//:org_apache_avro_avro",
        "@maven//:org_apache_commons_commons_compress",
        "@maven//:org_apache_commons_commons_csv",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_commons_commons_text",
        "@maven//:org_apache_httpcomponents_client5_httpclient5",
        "@maven//:org_apache_httpcomponents_client5_httpclient5_fluent",
        "@maven//:org_apache_httpcomponents_core5_httpcore5",
        "@maven//:org_apache_httpcomponents_httpcore",
        "@maven//:org_apache_kafka_connect_api",
        "@maven//:org_apache_kafka_connect_runtime",
        "@maven//:org_apache_kafka_connect_transforms",
        "@maven//:org_apache_kafka_kafka_2_13",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_apache_kafka_kafka_clients_test",
        "@maven//:org_apache_kafka_kafka_streams",
        "@maven//:org_apache_zookeeper_zookeeper",
        "@maven//:org_checkerframework_checker_qual",
        "@maven//:org_codehaus_janino_janino",
        "@maven//:org_easymock_easymock",
        "@maven//:org_hamcrest_hamcrest_all",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_inline",
        "@maven//:org_reactivestreams_reactive_streams",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:org_slf4j_slf4j_reload4j",
    ],
)

java_test(
    name = "MyTests",
    size = "large",
    test_class = "io.confluent.ksql.MyTestsEngine",
    runtime_deps = [":tests"],
)

java_test(
    name = "MyTestsFunction",
    test_class = "io.confluent.ksql.function.MyTestsEngineFunction",
    runtime_deps = [":tests"],
)

java_test(
    name = "MyTestsPlanner",
    test_class = "io.confluent.ksql.planner.MyTestsEnginePlanner",
    runtime_deps = [":tests"],
)

java_test(
    name = "MyTestsExecution",
    test_class = "io.confluent.ksql.execution.MyTestsEngineExecution",
    runtime_deps = [":tests"],
)

java_test(
    name = "MyTestsServices",
    test_class = "io.confluent.ksql.services.MyTestsEngineServices",
    runtime_deps = [":tests"],
)

java_test(
    name = "MyTestsAnalyzer",
    test_class = "io.confluent.ksql.analyzer.MyTestsEngineAnalyzer",
    runtime_deps = [":tests"],
)

java_test(
    name = "DependentStatementsIntegrationTest",
    test_class = "io.confluent.ksql.integration.DependentStatementsIntegrationTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "EndToEndIntegrationTest",
    test_class = "io.confluent.ksql.integration.EndToEndIntegrationTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "JoinIntTest",
    test_class = "io.confluent.ksql.integration.JoinIntTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "KafkaConsumerGroupClientTest",
    test_class = "io.confluent.ksql.integration.KafkaConsumerGroupClientTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "ReplaceIntTest",
    test_class = "io.confluent.ksql.integration.ReplaceIntTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "ReplaceWithSharedRuntimesIntTest",
    test_class = "io.confluent.ksql.integration.ReplaceWithSharedRuntimesIntTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "RetryTest",
    test_class = "io.confluent.ksql.integration.RetryTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "SecureIntegrationTest",
    test_class = "io.confluent.ksql.integration.SecureIntegrationTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "SelectValueMapperIntegrationTest",
    test_class = "io.confluent.ksql.integration.SelectValueMapperIntegrationTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "StreamsSelectAndProjectIntTest",
    size = "large",
    test_class = "io.confluent.ksql.integration.StreamsSelectAndProjectIntTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "UdfIntTest",
    test_class = "io.confluent.ksql.integration.UdfIntTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "WindowingIntTest",
    test_class = "io.confluent.ksql.integration.WindowingIntTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "WindowingSharedRuntimeIntTest",
    test_class = "io.confluent.ksql.integration.WindowingSharedRuntimeIntTest",
    runtime_deps = [":tests"],
)

java_test(
    name = "MyTestsUtil",
    test_class = "io.confluent.ksql.util.MyTestsEngineUtil",
    runtime_deps = [":tests"],
)

java_test(
    name = "MyTestsQuery",
    test_class = "io.confluent.ksql.query.MyTestsEngineQuery",
    runtime_deps = [":tests"],
)

java_test(
    name = "MyTestsStructured",
    test_class = "io.confluent.ksql.structured.MyTestsEngineStructured",
    runtime_deps = [":tests"],
)

java_test(
    name = "MyTestsEngine",
    test_class = "io.confluent.ksql.engine.MyTestsEngineEngine",
    runtime_deps = [":tests"],
)

java_test(
    name = "MyTestsSecurity",
    test_class = "io.confluent.ksql.security.MyTestsEngineSecurity",
    runtime_deps = [":tests"],
)
