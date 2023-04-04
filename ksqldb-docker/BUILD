# load("@io_bazel_rules_docker//container:container.bzl", "container_image")
# load("@io_bazel_rules_docker//container:container.bzl", "container_layer")
# load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

# container_layer(
#     name = "usr_bin_layer",
#     directory = "/usr/bin",
#     files = [
#         "//ksqldb-console-scripts:bin/ksql",
#         "//ksqldb-console-scripts:bin/ksql-datagen",
#         "//ksqldb-console-scripts:bin/ksql-migrations",
#         "//ksqldb-console-scripts:bin/ksql-print-metrics",
#         "//ksqldb-console-scripts:bin/ksql-restore-command-topic",
#         "//ksqldb-console-scripts:bin/ksql-run-class",
#         "//ksqldb-console-scripts:bin/ksql-server-start",
#         "//ksqldb-console-scripts:bin/ksql-server-stop",
#         "//ksqldb-console-scripts:bin/ksql-stop",
#         "//ksqldb-console-scripts:bin/ksql-test-runner",
#     ]
# )

# container_layer(
#     name = "usr_bin_docker_layer",
#     directory = "/usr/bin/docker",
#     files = [
#         ":src/include/docker/run",
#         ":src/include/docker/configure"
#     ],
# )

# container_layer(
#     name = "etc_layer",
#     directory = "/etc/ksqldb",
#     # todo is there a way to glob these?
#     # todo permission denied on these files. can a chmod be done?
#     files = [
#         "//ksqldb-etc:etc/connect.properties",
#         "//ksqldb-etc:etc/connect.properties.template",
#         "//ksqldb-etc:etc/datagen.properties",
#         "//ksqldb-etc:etc/ksql-migrations-log4j.properties",
#         "//ksqldb-etc:etc/ksql-production-server.properties",
#         "//ksqldb-etc:etc/ksql-server.properties",
#         "//ksqldb-etc:etc/ksqldb-server.properties.template",
#         "//ksqldb-etc:etc/log4j.properties",
#         "//ksqldb-etc:etc/log4j-file.properties",
#         "//ksqldb-etc:etc/log4j-rolling.properties",
#         "//ksqldb-etc:etc/log4j-silent.properties",
#     ],
# )

# container_layer(
#     name = "java_jar_layer",
#     directory = "/usr/share/java/ksqldb-rest-app",
#     files = [
#         "//ksqldb-rest-app:ksqldb_rest_app_deploy.jar",
#         "//ksqldb-cli:ksqldb_cli_deploy.jar",
#     ]
# )

# pkg_tar(
#     name = "confluent_hub_tar",
#     srcs = ["@confluent_hub_client//file"],
#     package_dir = "/home/appuser",
# )

# # use the @confluent_hub_client//file target to ouput a tar file for the container
# #genrule(
# #    name = "confluent_hub_tar",
# #    srcs = ["@confluent_hub_client//file"],
# #    outs = ["confluent_hub.tar"],
# #    cmd = "cp $< $@",
# #)

# container_image(
#     name = "ksqldb_image",
#     base = "@base//image",
#     cmd = ["/bin/docker/run"],
#     layers = [
#         ":usr_bin_layer",
#         ":usr_bin_docker_layer",
#         ":etc_layer",
#         ":java_jar_layer",
#     ],
#     files = [
#     ],
#     tars = [
# #        "confluent_hub_tar",
#         ":confluent_hub_tar",
# #        "@confluent_hub_client//file",
#     ],
#     data_path = ".",
#     # todo the original docker image would use user appuser
#     #  So far I have not found a way to write the files as appuser,
#     #  so we need to run as root in order to start up properly.
#     user = "root"

# )
