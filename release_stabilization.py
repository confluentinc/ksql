import logging
import os
import subprocess
import shlex
from collections import OrderedDict

DOCKER_REGISTRY = "confluent-docker-internal-stabilization.jfrog.io/"
DOCKER_UPSTREAM_REGISTRY = "confluent-docker-internal.jfrog.io/"
DOCKER_UPSTREAM_TAG = "v6.5.0"
PACKAGES_MAVEN_URL = r'${env.ORG_GRADLE_PROJECT_mavenUrl}'
CC_SPEC_KSQL_BRANCH = "master"
CCLOUD_DOCKER_REPO = 'confluentinc/cc-ksql'
CCLOUD_DOCKER_HOTFIX_REPO = 'confluentinc/cc-ksql-hotfix'
# need to update below automatically?
CP_VERSION = "7.1.0-cc-docker-ksql.11-88-ccs-rc1"
MAVEN_SKIP_TESTS = False
DOCKER_REPOS = ['confluentinc/ksqldb-cli', 'confluentinc/ksqldb-server']
DOCKER_ARTIFACT = 'confluentinc/ksqldb-docker'

class Callbacks:
    def __init__(self, working_dir, leaf, dry_run):
        self.leaf = leaf
        self.working_dir = working_dir

    """This is a callback to Confluent's cloud release tooling,
    and allows us to have consistent versioning"""
    def version_as_leaf(self):
        return self.leaf == 'cc-docker-ksql'

    def maven_build_args(self):
        build_args = ["integration-test", "-DversionFilter=true", "-U", "-Dspotbugs.skip", "-Dcheckstyle.skip"]
        return build_args

    def maven_deploy_args(self):
        deploy_args = ["-DversionFilter=true", "-U", "-DskipTests", "-DaltDeploymentRepository=confluent-artifactory-central::default::s3://staging-ksqldb-maven/maven",
                       "-DrepositoryId=confluent-artifactory-central", "-DnexusUrl=s3://staging-ksqldb-maven/maven"]
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
                subprocess.run(shlex.split(f"docker tag {DOCKER_UPSTREAM_REGISTRY}{DOCKER_ARTIFACT}:{v_version} {DOCKER_UPSTREAM_REGISTRY}{CCLOUD_DOCKER_REPO}:{v_version}"))
                subprocess.run(shlex.split(f"docker push {DOCKER_UPSTREAM_REGISTRY}{CCLOUD_DOCKER_REPO}:{v_version}"))

            return True
        except:
            return False