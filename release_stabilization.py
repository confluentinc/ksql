import os
import subprocess
import shlex
from collections import OrderedDict

DOCKER_INTERNAL_REGISTRY = "confluent-docker-internal.jfrog.io/"
DOCKER_REGISTRY = "confluent-docker.jfrog.io/"
CC_SPEC_KSQL_BRANCH = "master"
CCLOUD_DOCKER_REPO = 'confluentinc/cc-ksql'
CCLOUD_DOCKER_HOTFIX_REPO = 'confluentinc/cc-ksql-hotfix'
DOCKER_REPOS = ['confluentinc/ksqldb-cli', 'confluentinc/ksqldb-server']
DOCKER_ARTIFACT = 'confluentinc/ksqldb-docker'
KAFKA_TUTORIALS_BRANCH = 'ksqldb-latest'
KAFKA_TUTORIALS_URL = "git@github.com:confluentinc/kafka-tutorials.git"
GIT_CMD_KAFKA_TUTORIAL = "git --git-dir=./kafka-tutorials/.git --work-tree=./kafka-tutorials"

class Callbacks:
    def __init__(self, working_dir, leaf, dry_run):
        self.leaf = leaf
        self.working_dir = working_dir

    """This is a callback to Confluent's cloud release tooling,
    and allows us to have consistent versioning"""
    def version_as_leaf(self):
        return self.leaf == 'cc-docker-ksql'

    def maven_build_args(self):
        build_args = ["-DskipTests", "-DskipIntegrationTests", "-DversionFilter=true", "-U", "-Dspotbugs.skip", "-Dcheckstyle.skip"]
        return build_args

    def maven_deploy_args(self):
        deploy_args = ["-DskipTests", "-DskipIntegrationTests", "-DversionFilter=true", "-U"]
        return deploy_args

    def maven_docker_build(self):
        mvn_docker_args = OrderedDict()

        # defaults docker.tag to be version created by stabilization
        # mvn_docker_args["docker.tag"] =

        mvn_docker_args["docker.registry"] = DOCKER_REGISTRY
        mvn_docker_args["docker.test-registry"] = DOCKER_REGISTRY
        mvn_docker_args["docker.upstream-registry"] = ""
        mvn_docker_args["docker.upstream-tag"] = "7.0.1"
        mvn_docker_args["skip.docker.build"] = "false"
        mvn_docker_args["skip.docker.test"] = "true"
        return mvn_docker_args

    def after_publish(self, version: str) -> bool:
        v_version = "v" + version
        try:

            # is a final version build
            if "rc" not in version:
                # promote production images to dockerhub
                for docker_repo in DOCKER_REPOS:
                    print(f"docker pull {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version}")
                    subprocess.run(shlex.split(f"docker pull {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version}"))

                    print(f"docker tag {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version} {docker_repo}:{version}")
                    subprocess.run(shlex.split(f"docker tag {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version} {docker_repo}:{version}"))
                    print(f"docker push {docker_repo}:{version}")
                    subprocess.run(shlex.split(f"docker push {docker_repo}:{version}"))

                    # update latest tag images on dockerhub
                    print(f"docker tag {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version} {docker_repo}:latest")
                    subprocess.run(shlex.split(f"docker tag {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version} {docker_repo}:latest"))
                    print(f"docker push {docker_repo}:latest")
                    subprocess.run(shlex.split(f"docker push {docker_repo}:latest"))

            # pull, tag, and push latest docker on-prem images
            for docker_repo in DOCKER_REPOS:
                print(f"docker tag {DOCKER_REGISTRY}{DOCKER_ARTIFACT}:{version} {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version}")
                subprocess.run(shlex.split(f"docker tag {DOCKER_REGISTRY}{DOCKER_ARTIFACT}:{version} {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version}"))
                print(f"docker push {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version}")
                subprocess.run(shlex.split(f"docker push {DOCKER_INTERNAL_REGISTRY}{docker_repo}:{version}"))

            # clone kafka tutorials and checkout 'ksqldb-latest'
            git_cmd_kafka_tutorial = f"git --git-dir={self.working_dir}/kafka-tutorials/.git --work-tree={self.working_dir}/kafka-tutorials"
            print(f"{git_cmd_kafka_tutorial}")

            kafka_tutorials_cwd = os.path.join(self.working_dir, 'kafka-tutorials')
            print(f"{kafka_tutorials_cwd}")

            print(f"git clone {KAFKA_TUTORIALS_URL} {kafka_tutorials_cwd}")
            subprocess.run(shlex.split(f"git clone {KAFKA_TUTORIALS_URL} {kafka_tutorials_cwd}"))

            print(f"{git_cmd_kafka_tutorial} checkout {KAFKA_TUTORIALS_BRANCH}")
            subprocess.run(shlex.split(f"{git_cmd_kafka_tutorial} checkout {KAFKA_TUTORIALS_BRANCH}"), cwd=kafka_tutorials_cwd)

            # update kafka tutorials and kick off semaphore test
            print(f"{kafka_tutorials_cwd}")
            print(f"{self.working_dir}")
            update_ksqldb_version_path = os.path.join(self.working_dir, 'kafka-tutorials/tools/update-ksqldb-version.sh')
            print(f"{update_ksqldb_version_path} {version} {DOCKER_REGISTRY}")
            subprocess.run(shlex.split(f"{update_ksqldb_version_path} {version} {DOCKER_REGISTRY}"))

            print(f"{git_cmd_kafka_tutorial} diff")
            subprocess.run(shlex.split(f"{git_cmd_kafka_tutorial} diff"), cwd=kafka_tutorials_cwd)
            print(f"{git_cmd_kafka_tutorial} add _includes/*")
            subprocess.run(shlex.split(f"{git_cmd_kafka_tutorial} add _includes/*"), cwd=kafka_tutorials_cwd)
            print(f"{git_cmd_kafka_tutorial} commit --allow-empty -m \"build: set ksql version to {version}\"")
            subprocess.run(shlex.split(f"{git_cmd_kafka_tutorial} commit --allow-empty -m \"build: set ksql version to {version}\""), cwd=kafka_tutorials_cwd)
            print(f"{git_cmd_kafka_tutorial} push origin HEAD:{KAFKA_TUTORIALS_BRANCH}")
            subprocess.run(shlex.split(f"{git_cmd_kafka_tutorial} push origin HEAD:{KAFKA_TUTORIALS_BRANCH}"), cwd=kafka_tutorials_cwd)

            return True
        except:
            return False
