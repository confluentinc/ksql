# JVM External
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

RULES_JVM_EXTERNAL_TAG = "4.2"
RULES_JVM_EXTERNAL_SHA = "cd1a77b7b02e8e008439ca76fd34f5b07aecb8c752961f9640dea15e9e5ba1ca"

http_archive(
    name = "rules_jvm_external",
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    sha256 = RULES_JVM_EXTERNAL_SHA,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

load("@rules_jvm_external//:defs.bzl", "maven_install")

# todo how to exclude slf4j from transitives
maven_install(
    artifacts = [
        "com.google.guava:guava:30.1.1-jre",
        "com.google.guava:guava-testlib:30.1.1-jre",
        "com.github.spotbugs:spotbugs-annotations:4.2.3",
        "junit:junit:4.13.1",
        "org.hamcrest:hamcrest-all:1.3",
        "com.google.errorprone:error_prone_annotations:2.2.0",
        "org.apache.curator:curator-test:5.2.0",
        "org.apache.kafka:kafka_2.13:7.4.0-30-ccs",
        "org.apache.kafka:kafka_2.13:jar:test:7.4.0-30-ccs",
        "org.apache.kafka:kafka-clients:7.4.0-30-ccs",
        "org.apache.kafka:kafka-clients:jar:test:7.4.0-30-ccs",
        "org.easymock:easymock:4.3",
        "org.mockito:mockito-inline:4.6.1",
        "org.mockito:mockito-core:4.6.1",
        "com.google.guava:guava-testlib:30.1.1-jre",
        "org.slf4j:slf4j-reload4j:1.7.36",
        "org.slf4j:slf4j-api:1.7.36",
        "io.confluent:logredactor:1.0.10",
        "org.apache.zookeeper:zookeeper:3.6.3",
        "org.scala-lang:scala-library:2.13.6",
    ],
    repositories = [
        # Private repositories are supported through HTTP Basic auth
        # "http://username:password@localhost:8081/artifactory/my-repository",
       "https://confluent.jfrog.io/confluent/maven-public",
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
