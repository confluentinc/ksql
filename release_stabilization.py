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

            # pull, tag, and push latest docker images
            for docker_repo in DOCKER_REPOS:
                print(f"docker tag {DOCKER_UPSTREAM_REGISTRY}{DOCKER_ARTIFACT}:{version} {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{v_version}")
                subprocess.run(shlex.split(f"docker tag {DOCKER_UPSTREAM_REGISTRY}{DOCKER_ARTIFACT}:{version} {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{v_version}"))
                print(f"docker push {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{v_version}")
                subprocess.run(shlex.split(f"docker push {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{v_version}"))

            return True
        except:
            return False