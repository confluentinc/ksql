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

class Callbacks:
    def __init__(self, working_dir, leaf, dry_run):
        self.leaf = leaf
        self.working_dir = working_dir

    """This is a callback to Confluent's cloud release tooling,
    and allows us to have consistent versioning"""
    def version_as_leaf(self):
        return self.leaf == 'cc-docker-ksql'

    def maven_docker_build(self):
        mvn_docker_args = OrderedDict()
        mvn_docker_args["docker.registry"] = DOCKER_REGISTRY
        mvn_docker_args["docker.test-registry"] = DOCKER_REGISTRY
        mvn_docker_args["docker.upstream-registry"] = DOCKER_UPSTREAM_REGISTRY
        mvn_docker_args["docker.upstream-tag"] = DOCKER_UPSTREAM_TAG
        mvn_docker_args["skip.docker.build"] = "false"
        return mvn_docker_args

    def after_build(self, version: str) -> bool:
        v_version = "v" + version
        try:
            # Install utilities required for building. XXX: Add to base image
            subprocess.run(shlex.split("sudo apt update"))
            subprocess.run(shlex.split("sudo apt install -y devscripts git-buildpackage dh-systemd javahelper xmlstarlet"))

            os.environ["MAVEN_OPTS"] = "-XX:MaxPermSize=128M"
            settings_file_path = os.path.join(self.working_dir, "maven-settings.xml")

            subprocess.run(shlex.split(f"cp {settings_file_path} ~/.m2/settings.xml"))
            subprocess.run(shlex.split(f"cp {settings_file_path} {self.working_dir}"))

            build_packages_path = os.path.join(self.working_dir, "build_packages.sh")
            subprocess.run(shlex.split(f"{build_packages_path} --workspace {self.working_dir} "
                f"--docker-registry {DOCKER_REGISTRY} " +
                f"--project-version {version} "
                f"--upstream-version ${CP_VERSION} --jar " +
                f"{'--skip-maven-tests' if MAVEN_SKIP_TESTS else ''}"))

            # is a final build rc release
            if "rc" not in version:
                # Publish Artifacts
                subprocess.run(shlex.split(f"mvn --batch-mode -Pjenkins deploy -DskipTests -Ddocker.skip-build=true -Ddocker.skip-test=true "
                                           f"-DaltDeploymentRepository=confluent-artifactory-central::default::s3://staging-ksqldb-maven/maven " +
                                           f"-DrepositoryId=confluent-artifactory-central "
                                           f"-DnexusUrl=s3://staging-ksqldb-maven/maven "))

            return True
        except:
            return False