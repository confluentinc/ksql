#!/bin/bash

set -e

repos=('kafka' 'common' 'ce-kafka' 'rest-utils' 'schema-registry' 'ksql' 'kafka-rest')
repos+=('secret-registry' 'confluent-security-plugins' 'schema-registry-plugins')
repos+=('confluent-cloud-plugins' 'cc-docker-ksql')

branches=('7.1.0-cc-docker-ksql.17-99-ccs.x' '7.1.0-cc-docker-ksql.17-634.x' '7.1.0-cc-docker-ksql.17-613-ce.x')
branches+=('7.1.0-cc-docker-ksql.17-615.x' '7.1.0-cc-docker-ksql.17-644.x' '0.23.1-cc-docker-ksql.17.x')
branches+=('7.1.0-cc-docker-ksql.17-609.x' '7.1.0-cc-docker-ksql.17-559.x' '7.1.0-cc-docker-ksql.17-1524.x')
branches+=('7.1.0-cc-docker-ksql.17-684.x' '7.1.0-cc-docker-ksql.17-1730.x' '0.23.1-cc-docker-ksql.17.x')

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
    deploy_cmd="mvn --batch-mode -Pjenkins deploy -DskipTests -Ddocker.skip-build=true -Ddocker.skip-test=true"
    deploy_cmd+=" -DaltDeploymentRepository=confluent-artifactory-central::default::s3://staging-ksqldb-maven/maven"
    deploy_cmd+=" -DrepositoryId=confluent-artifactory-central"
    deploy_cmd+=" -DnexusUrl=s3://staging-ksqldb-maven/maven"
    echo $deploy_cmd
    eval $deploy_cmd

    echo "cd .."
    eval cd ..
  elif [[ -e 'build.gradle' ]]
    # gradle file means this is a gradle project
    deploy_cmd="./gradlewAll publish -PmavenUrl=s3://staging-ksqldb-maven/maven -PignoreFailures -PskipSigning"
    echo $deploy_cmd
    eval $deploy_cmd

    echo "cd .."
    eval cd ..
  fi
done

# git clone each repo in list

# checkout CORRECT stabilization branch for each repo

# run mvn deploy on each branch
