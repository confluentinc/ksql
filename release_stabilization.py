import logging
import os
import subprocess
import shlex
from collections import OrderedDict

DOCKER_REGISTRY = "confluent-docker-internal-stabilization.jfrog.io/"
DOCKER_INTERNAL_REGISTRY = "confluent-docker-internal.jfrog.io/"
DOCKER_UPSTREAM_REGISTRY = "confluent-docker.jfrog.io/"
DOCKER_UPSTREAM_TAG = "v6.5.0"
PACKAGES_MAVEN_URL = r'${env.ORG_GRADLE_PROJECT_mavenUrl}'
CC_SPEC_KSQL_BRANCH = "master"
CCLOUD_DOCKER_REPO = 'confluentinc/cc-ksql'
CCLOUD_DOCKER_HOTFIX_REPO = 'confluentinc/cc-ksql-hotfix'
# need to update below automatically?
CP_VERSION = "7.1.0-cc-docker-ksql.17-99-ccs-rc1"
MAVEN_SKIP_TESTS = False
DOCKER_REPOS = ['confluentinc/ksqldb-cli', 'confluentinc/ksqldb-server']
DOCKER_ARTIFACT = 'confluentinc/ksqldb-docker'
KAFKA_TUTORIALS_BRANCH = 'ksqldb-latest'
KAFKA_TUTORIALS_URL = "git@github.com:confluentinc/kafka-tutorials.git"
GIT_CMD_KAFKA_TUTORIAL = "git --git-dir=./kafka-tutorials/.git --work-tree=./kafka-tutorials"

class Callbacks:
    def __init__(self, working_dir, leaf, dry_run):
        self.leaf = leaf
        self.working_dir = working_dir
        self.settings_path = os.path.join(self.working_dir, 'maven-settings.xml')

    """This is a callback to Confluent's cloud release tooling,
    and allows us to have consistent versioning"""
    def version_as_leaf(self):
        return self.leaf == 'cc-docker-ksql'

    def maven_build_args(self):
        build_args = ["-gs", f"{self.settings_path}", "-DskipTests", "-DskipIntegrationTests", "-DversionFilter=true", "-U", "-Dspotbugs.skip", "-Dcheckstyle.skip"]
        return build_args

    def maven_deploy_args(self):
        deploy_args = ["-gs", f"{self.settings_path}", "-DskipIntegrationTests", "-DversionFilter=true", "-U", "-DskipTests"]
        return deploy_args

    def maven_docker_build(self):
        mvn_docker_args = OrderedDict()

        # defaults docker.tag to be version created by stabilization
        # mvn_docker_args["docker.tag"] =

        mvn_docker_args["docker.registry"] = DOCKER_UPSTREAM_REGISTRY
        mvn_docker_args["docker.test-registry"] = DOCKER_UPSTREAM_REGISTRY
        mvn_docker_args["docker.upstream-tag"] = CP_VERSION + "-latest"
        mvn_docker_args["skip.docker.build"] = "false"
        mvn_docker_args["skip.docker.test"] = "true"
        return mvn_docker_args

    def after_publish(self, version: str) -> bool:
        v_version = "v" + version
        try:

            # pull, tag, and push latest docker on prem images
            for docker_repo in DOCKER_REPOS:
                print(f"docker tag {DOCKER_UPSTREAM_REGISTRY}{DOCKER_ARTIFACT}:{version} {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version}")
                subprocess.run(shlex.split(f"docker tag {DOCKER_UPSTREAM_REGISTRY}{DOCKER_ARTIFACT}:{version} {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version}"))
                print(f"docker push {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version}")
                subprocess.run(shlex.split(f"docker push {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version}"))

            git_cmd_kafka_tutorial = f"git --git-dir={self.working_dir}/kafka-tutorials/.git --work-tree={self.working_dir}/kafka-tutorials"
            print(f"{git_cmd_kafka_tutorial}")
            # clone kafka tutorials and checkout 'ksqldb-latest'
            kafka_tutorials_cwd = os.path.join(self.working_dir, 'kafka-tutorials')
            print(f"{kafka_tutorials_cwd}")

            print(f"{git_cmd_kafka_tutorial} clone {KAFKA_TUTORIALS_URL} {kafka_tutorials_cwd}")
            subprocess.run(shlex.split(f"{git_cmd_kafka_tutorial} clone {KAFKA_TUTORIALS_URL} {kafka_tutorials_cwd}"))
            # print(f"cd kafka-tutorials")
            # subprocess.run(shlex.split(f"cd kafka-tutorials"))


            print(f"{git_cmd_kafka_tutorial} checkout {KAFKA_TUTORIALS_BRANCH}")
            subprocess.run(shlex.split(f"{git_cmd_kafka_tutorial} checkout {KAFKA_TUTORIALS_BRANCH}"), cwd=kafka_tutorials_cwd)

            # update kafka tutorials and kick off semaphore test
            update_ksqldb_version_path = os.path.join(self.working_dir, '/tools/update-ksqldb-version.sh')
            print(f"{update_ksqldb_version_path} {version} {DOCKER_INTERNAL_REGISTRY}")
            subprocess.run(shlex.split(f"{update_ksqldb_version_path} {version} {DOCKER_INTERNAL_REGISTRY}"))

            print(f"{git_cmd_kafka_tutorial} diff")
            subprocess.run(shlex.split(f"{git_cmd_kafka_tutorial} diff"), cwd=kafka_tutorials_cwd)
            print(f"{git_cmd_kafka_tutorial} add _includes/*")
            subprocess.run(shlex.split(f"{git_cmd_kafka_tutorial} add _includes/*"), cwd=kafka_tutorials_cwd)
            print(f"{git_cmd_kafka_tutorial} commit --allow-empty -m \"build: set ksql version to ${version}\"")
            subprocess.run(shlex.split(f"{git_cmd_kafka_tutorial} commit --allow-empty -m \"build: set ksql version to ${version}\""), cwd=kafka_tutorials_cwd)
            print(f"{git_cmd_kafka_tutorial} push origin HEAD:{KAFKA_TUTORIALS_BRANCH}")
            subprocess.run(shlex.split(f"{git_cmd_kafka_tutorial} push origin HEAD:{KAFKA_TUTORIALS_BRANCH}"), cwd=kafka_tutorials_cwd)

            return True
        except:
            return False