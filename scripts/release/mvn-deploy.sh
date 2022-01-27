#!/bin/bash

set -e

MY_DIR=`echo $(cd $(dirname $0); pwd)`

# list of cc-docker-ksql dependencies. This will eventually be automatically parsed from the output
# file of the release stabilization jenkins job
repos=('kafka' 'common' 'ce-kafka' 'rest-utils' 'schema-registry' 'ksql' 'kafka-rest')
repos+=('secret-registry' 'confluent-security-plugins' 'schema-registry-plugins')
repos+=('confluent-cloud-plugins' 'cc-docker-ksql')

#repos=('kafka' 'common' 'ce-kafka' 'rest-utils' 'schema-registry' 'ksql' 'kafka-rest')
#repos=('kafka-rest' 'secret-registry' 'confluent-security-plugins' 'schema-registry-plugins')
#repos+=('confluent-cloud-plugins' 'cc-docker-ksql')

# list of the corresponding stabilization branch for each respective repo above.
# Should replace this with git branch --all --list '*-cc-docker-ksql.17-*' though
branches=('7.1.0-cc-docker-ksql.17-99-ccs.x' '7.1.0-cc-docker-ksql.17-634.x' '7.1.0-cc-docker-ksql.17-613-ce.x')
branches+=('7.1.0-cc-docker-ksql.17-615.x' '7.1.0-cc-docker-ksql.17-644.x' '0.23.1-cc-docker-ksql.17.x')
branches+=('7.1.0-cc-docker-ksql.17-609.x' '7.1.0-cc-docker-ksql.17-559.x' '7.1.0-cc-docker-ksql.17-1524.x')
branches+=('7.1.0-cc-docker-ksql.17-684.x' '7.1.0-cc-docker-ksql.17-1730.x' '0.23.1-cc-docker-ksql.17.x')

#branches=('7.1.0-cc-docker-ksql.17-609.x' '7.1.0-cc-docker-ksql.17-559.x' '7.1.0-cc-docker-ksql.17-1524.x')
#branches+=('7.1.0-cc-docker-ksql.17-684.x' '7.1.0-cc-docker-ksql.17-1730.x' '0.23.1-cc-docker-ksql.17.x')

len=${#repos[@]}

for (( i=0; i<$len; i++ ));
do
  echo $i

  echo "git clone git@github.com:confluentinc/${repos[i]}.git ./${repos[i]}"
  git clone git@github.com:confluentinc/${repos[i]}.git ./${repos[i]}

  gitcmd="git --git-dir=./${repos[i]}/.git --work-tree=./${repos[i]}"
  echo "$gitcmd checkout ${branches[i]}"
  $gitcmd checkout ${branches[i]}

  echo "cd ${repos[i]}"
  eval cd ${repos[i]}

  if [[ -e 'pom.xml' ]]
  then
    # pom file means this is a maven project
    eval pwd
    if [[ "${repos[i]}" == "common" ]]
    then
      echo "patch -p1 --ignore-whitespace --verbose < ${MY_DIR}/common-deploy.patch"
      patch -p1 --ignore-whitespace --verbose < ${MY_DIR}/common-deploy.patch
      find . -name '*.rej'
    elif [[ "${repos[i]}" != "rest-utils" ]]
    then
      echo "patch -p1 --ignore-whitespace --verbose < ${MY_DIR}/s3-deploy.patch"
      patch -p1 --ignore-whitespace --verbose < ${MY_DIR}/s3-deploy.patch
      find . -name '*.rej'
    fi
    
    mvn_cmd="mvn help:effective-pom"
    echo $mvn_cmd
    eval $mvn_cmd
    deploy_cmd="mvn --batch-mode -Pjenkins deploy -DskipTests -Ddocker.skip-build=true -Ddocker.skip-test=true"
    deploy_cmd+=" -DaltDeploymentRepository=confluent-artifactory-central::default::s3://staging-ksqldb-maven/maven"
    deploy_cmd+=" -DrepositoryId=confluent-artifactory-central"
    deploy_cmd+=" -DnexusUrl=s3://staging-ksqldb-maven/maven"
    echo $deploy_cmd
    eval $deploy_cmd

    echo "cd .."
    eval cd ..
  elif [[ -e 'build.gradle' ]]
  then
    # gradle file means this is a gradle project


    eval pwd
#    echo "git apply --recount --whitespace=fix ${MY_DIR}/kafka-deploy.patch"
#    git apply --recount --whitespace=warn ${MY_DIR}/kafka-deploy.patch
    eval ls
    echo "patch -p1 --verbose < ${MY_DIR}/kafka-deploy.patch"
    patch -p1 --verbose < ${MY_DIR}/kafka-deploy.patch
    find . -name '*.rej'

    eval ls

    deploy_cmd="./gradlewAll --init-script ${GRADLE_NEXUS_SETTINGS} --no-daemon"
    deploy_cmd+=" -PmavenUrl=s3://staging-ksqldb-maven/maven -PskipSigning=true uploadArchives"
    echo $deploy_cmd
    eval $deploy_cmd

    echo "cd .."
    eval cd ..
  fi
done

# git clone each repo in list

# checkout CORRECT stabilization branch for each repo

# run mvn deploy on each branch
