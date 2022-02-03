#!/bin/bash -x

# Builds all the Java maven artifacts for each ksql dependency and deploys them as a maven
# repository to a staging S3 bucket.

set -e
stabilization_unique_identifier=$1
MY_DIR=`echo $(cd $(dirname $0); pwd)`

# list of ksql dependencies
repos=('kafka' 'common' 'rest-utils' 'schema-registry' 'ksql')
len=${#repos[@]}

for (( i=0; i<$len; i++ ));
do
  # clone each repo and checkout the stabilization branch
  git clone git@github.com:confluentinc/${repos[i]}.git ./${repos[i]}
  gitcmd="git --git-dir=./${repos[i]}/.git --work-tree=./${repos[i]}"
  eval branch=$($gitcmd branch --list "*${stabilization_unique_identifier}*")
  $gitcmd switch $branch

  cd ${repos[i]}

  # pom file means this is a maven project
  if [[ -e "pom.xml" ]]
  then
    if [[ "${repos[i]}" == "common" ]]
    then
      # apply a patch to pom so that we can deploy to s3 with correct permissions
      patch -p1 --ignore-whitespace --verbose < ${MY_DIR}/common-deploy.patch
    elif [[ "${repos[i]}" == "rest-utils" ]]
    then
      patch -p1 --ignore-whitespace --verbose < ${MY_DIR}/plugins-deploy.patch
    elif [[ "${repos[i]}" == "ksql" || "${repos[i]}" == "schema-registry" ]]
    then
      patch -p1 --ignore-whitespace --verbose < ${MY_DIR}/pluginManagement-deploy.patch
    fi

    deploy_cmd="mvn --batch-mode -Pjenkins deploy -DskipTests -Ddocker.skip-build=true -Ddocker.skip-test=true"
    deploy_cmd+=" -DaltDeploymentRepository=confluent-artifactory-central::default::s3://staging-ksqldb-maven/maven"
    deploy_cmd+=" -DrepositoryId=confluent-artifactory-central"
    deploy_cmd+=" -DnexusUrl=s3://staging-ksqldb-maven/maven"
    $deploy_cmd

    cd ..

  # gradle file means this is a gradle project
  elif [[ -e 'build.gradle' ]]
  then
    patch -p1 --verbose < ${MY_DIR}/kafka-deploy.patch
    deploy_cmd="./gradlewAll --init-script ${GRADLE_NEXUS_SETTINGS} --no-daemon"
    deploy_cmd+=" -PmavenUrl=s3://staging-ksqldb-maven/maven -PskipSigning=true uploadArchives"
    $deploy_cmd

    cd ..
  fi
done

