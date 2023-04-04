# pkg rules
# https://github.com/bazelbuild/rules_pkg/issues/606 had to be at the top because of this
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_pkg",
    sha256 = "8a298e832762eda1830597d64fe7db58178aa84cd5926d76d5b744d6558941c2",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
    ],
)

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

# JVM External
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

RULES_JVM_EXTERNAL_TAG = "4.2"

RULES_JVM_EXTERNAL_SHA = "cd1a77b7b02e8e008439ca76fd34f5b07aecb8c752961f9640dea15e9e5ba1ca"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

maven_install(
    artifacts = [
        "com.google.guava:guava:30.1.1-jre",
        "com.google.guava:guava-testlib:30.1.1-jre",
        "com.github.spotbugs:spotbugs-annotations:4.2.3",
        "junit:junit:4.13.1",
        "org.hamcrest:hamcrest-all:1.3",
        "com.google.errorprone:error_prone_annotations:2.2.0",
        "org.apache.kafka:kafka_2.13:7.4.0-33-ccs",
        "org.apache.kafka:kafka_2.13:jar:test:7.4.0-33-ccs",
        "org.apache.kafka:kafka-clients:7.4.0-33-ccs",
        "org.apache.kafka:kafka-clients:jar:test:7.4.0-33-ccs",
        "org.easymock:easymock:4.3",
        "org.mockito:mockito-inline:4.6.1",
        "org.mockito:mockito-core:4.6.1",
        "com.google.guava:guava-testlib:30.1.1-jre",
        "org.slf4j:slf4j-reload4j:1.7.36",
        "io.confluent:logredactor:1.0.10",
        "org.apache.zookeeper:zookeeper:3.6.3",
        "org.scala-lang:scala-library:2.13.6",
        "com.google.code.findbugs:jsr305:3.0.2",
        "io.confluent:kafka-connect-avro-converter:7.4.0-124",
        "io.confluent:common-logging:7.4.0-128",
        "org.apache.kafka:kafka-streams:7.4.0-33-ccs",
        "org.apache.kafka:kafka-streams-test-utils:7.4.0-33-ccs",
        "org.apache.avro:avro:1.11.0",
        "org.apache.commons:commons-lang3:3.5",
        "io.vertx:vertx-core:4.3.2",
        "io.vertx:vertx-codegen:4.3.2",
        "io.vertx:vertx-web:4.3.2",
        "org.reactivestreams:reactive-streams:1.0.3",
        "org.reactivestreams:reactive-streams-tck:1.0.3",
        "com.fasterxml.jackson.core:jackson-core:2.13.2",
        "com.fasterxml.jackson.core:jackson-databind:2.13.2",
        "com.fasterxml.jackson.core:jackson-annotations:2.13.2",
        "com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.13.2",
        "com.fasterxml.jackson.datatype:jackson-datatype-guava:2.13.2",
        "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.2",
        "org.apache.kafka:connect-api:7.4.0-33-ccs",
        "io.confluent:common-utils:7.4.0-128",
        "org.apache.kafka:connect-json:7.4.0-33-ccs",
        "javax.xml.bind:jaxb-api:2.3.1",
        "org.apache.commons:commons-csv:1.4",
        "io.confluent:kafka-schema-registry-client:7.4.0-124",
        "io.confluent:kafka-protobuf-provider:7.4.0-124",
        "io.confluent:kafka-protobuf-serializer:7.4.0-124",
        "io.confluent:kafka-json-schema-provider:7.4.0-124",
        "io.confluent:kafka-connect-protobuf-converter:7.4.0-124",
        "io.confluent:kafka-connect-json-schema-converter:7.4.0-124",
        "io.confluent:kafka-avro-serializer:7.4.0-124",
        "com.google.protobuf:protobuf-java:3.19.4",
        "org.apache.kafka:connect-api:7.4.0-33-ccs",
        "io.confluent:kafka-connect-avro-data:7.4.0-124",
        "com.squareup.wire:wire-schema-jvm:4.3.0",
        "io.confluent:kafka-protobuf-types:7.4.0-124",
        "com.google.api.grpc:proto-google-common-protos:2.5.1",
        "javax.ws.rs:javax.ws.rs-api:2.1.1",
        "io.netty:netty-codec-http:4.1.78.Final",
        "org.codehaus.janino:janino:3.0.7",
        "org.codehaus.janino:commons-compiler:3.0.7",
        "com.ibm.icu:icu4j:67.1",
        "io.airlift:slice:0.29",
        "org.antlr:antlr4-runtime:4.9.2",
        "org.antlr:antlr4:4.9.2",
        "org.apache.commons:commons-text:1.8",
        "com.approvaltests:approvaltests:9.5.0",
        "org.apache.commons:commons-compress:1.21",
        "com.github.rholder:guava-retrying:2.0.0",
        "org.apache.httpcomponents.client5:httpclient5-fluent:5.0.3",
        "io.github.classgraph:classgraph:4.8.59",
        "com.squareup:javapoet:1.9.0",
        "com.clearspring.analytics:stream:2.9.5",
        "io.confluent.avro:avro-random-generator:0.2.2",
        "com.github.tomakehurst:wiremock-jre8:2.24.0",
        "io.confluent:confluent-log4j-extensions:7.4.0-128",
        "commons-codec:commons-codec:1.13",
        "com.github.rvesse:airline:2.6.0",
        "io.vertx:vertx-dropwizard-metrics:4.3.2",
        "org.eclipse.jetty:jetty-jaas:9.4.44.v20210927",
        "io.vertx:vertx-web-client:4.3.2",
        "io.vertx:vertx-web-common:4.3.2",
        "org.jeasy:easy-random-core:4.2.0",
        "org.eclipse.jetty:jetty-http:9.4.48.v20220622",
        "org.rocksdb:rocksdbjni:6.29.4.1",
        "org.jline:jline:3.13.1",
        "commons-io:commons-io:2.7",
        "net.sf.jopt-simple:jopt-simple:5.0.4",
        "org.openjdk.jmh:jmh-core:1.21",
        "org.openjdk.jmh:jmh-generator-annprocess:1.21",
        "org.testng:testng:6.11",
        maven.artifact(
            artifact = "connect-runtime",
            exclusions = [
                maven.exclusion(
                    artifact = "log4j-core",
                    group = "org.apache.logging.log4j",
                ),
                maven.exclusion(
                    artifact = "log4j-api",
                    group = "org.apache.logging.log4j",
                ),
            ],
            group = "org.apache.kafka",
            version = "7.4.0-33-ccs",
        ),
        maven.artifact(
            artifact = "curator-test",
            exclusions = [
                maven.exclusion(
                    artifact = "log4j",
                    group = "log4j",
                ),
            ],
            group = "org.apache.curator",
            version = "5.1.0",
        ),
    ],
    excluded_artifacts = [
        "org.slf4j:slf4j-log4j12",
    ],
    repositories = [
        # Private repositories are supported through HTTP Basic auth
        # "http://username:password@localhost:8081/artifactory/my-repository",
        "https://confluent-519856050701.dp.confluent.io/maven/maven-public/",
        "https://confluent-519856050701.d.codeartifact.us-west-2.amazonaws.com/maven/maven/",
        "https://packages.confluent.io/maven/",
        "https://repo1.maven.org/maven2/",
    ],
)

# Scala
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

skylib_version = "1.0.3"

http_archive(
    name = "bazel_skylib",
    sha256 = "1c531376ac7e5a180e0237938a2536de0c54d93f5c278634818e0efc952dd56c",
    type = "tar.gz",
    url = "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/{}/bazel-skylib-{}.tar.gz".format(skylib_version, skylib_version),
)

rules_scala_version = "e7a948ad1948058a7a5ddfbd9d1629d6db839933"

http_archive(
    name = "io_bazel_rules_scala",
    sha256 = "76e1abb8a54f61ada974e6e9af689c59fd9f0518b49be6be7a631ce9fa45f236",
    strip_prefix = "rules_scala-%s" % rules_scala_version,
    type = "zip",
    url = "https://github.com/bazelbuild/rules_scala/archive/%s.zip" % rules_scala_version,
)

# Stores Scala version and other configuration
# 2.12 is a default version, other versions can be use by passing them explicitly:
# scala_config(scala_version = "2.11.12")
load("@io_bazel_rules_scala//:scala_config.bzl", "scala_config")

scala_config()

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")

scala_repositories()

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")

scala_register_toolchains()

# optional: setup ScalaTest toolchain and dependencies
load("@io_bazel_rules_scala//testing:scalatest.bzl", "scalatest_repositories", "scalatest_toolchain")

scalatest_repositories()

# Antlr
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_antlr",
    sha256 = "26e6a83c665cf6c1093b628b3a749071322f0f70305d12ede30909695ed85591",
    strip_prefix = "rules_antlr-0.5.0",
    urls = ["https://github.com/marcohu/rules_antlr/archive/0.5.0.tar.gz"],
)

load("@rules_antlr//antlr:repositories.bzl", "rules_antlr_dependencies")

rules_antlr_dependencies("4.7.2")

# Avro
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

#RULES_JVM_EXTERNAL_TAG = "4.1"

#RULES_JVM_EXTERNAL_SHA = "f36441aa876c4f6427bfb2d1f2d723b48e9d930b62662bf723ddfb8fc80f0140"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

RULES_AVRO_VERSION = "a4c607a5610bea5649b1fb466ea8abcd9916121b"

RULES_AVRO_SHA256 = "aebc8fc6f8a6a3476d8e8f6f6878fc1cf7a253399e1b2668963e896512be1cc6"

http_archive(
    name = "io_bazel_rules_avro",
    sha256 = RULES_AVRO_SHA256,
    strip_prefix = "rules_avro-%s" % RULES_AVRO_VERSION,
    url = "https://github.com/chenrui333/rules_avro/archive/%s.tar.gz" % RULES_AVRO_VERSION,
)

load("@io_bazel_rules_avro//avro:avro.bzl", "avro_repositories")

avro_repositories()

# Docker
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Go (necessary to avoid issue described in this thread)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "16e9fca53ed6bd4ff4ad76facc9b7b651a89db1689a2877d6fd7b82aa824e366",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.34.0/rules_go-v0.34.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.34.0/rules_go-v0.34.0.zip",
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains(version = "1.18.3")

# Docker (setup as described in rules_docker repo example)

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "b1e80761a8a8243d03ebca8845e9cc1ba6c82ce7c5179ce2b295cd36f7e394bf",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.25.0/rules_docker-v0.25.0.tar.gz"],
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

# container_pull(
#     name = "base",
#     registry = "docker.io",
#     repository = "confluentinc/cp-base-new",
#     # 'tag' is also supported, but digest is encouraged for reproducibility.
#     #    digest = "sha256:deadbeef",
#     tag = "latest",
# )

# confluent hub for docker container
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")

CONFLUENT_HUB_CLIENT_VERSION = "latest"

CONFLUENT_HUB_CLIENT_SHA256 = "5513032967e2b75af7eb527ac37b64ab7fb018fb91c9d23006542752799e0cb5"

http_file(
    name = "confluent_hub_client",
    executable = True,
    sha256 = CONFLUENT_HUB_CLIENT_SHA256,
    urls = ["http://client.hub.confluent.io/confluent-hub-client-{}.tar.gz".format(CONFLUENT_HUB_CLIENT_VERSION)],
)
