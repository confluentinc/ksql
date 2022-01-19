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
  echo $i
  git clone git@github.com:confluentinc/${repos[i]}.git ./${repos[i]}
  echo "git clone git@github.com:confluentinc/${repos[i]}.git ./${repos[i]}"

  gitcmd="git --git-dir=./${repos[i]}/.git --work-tree=./${repos[i]}"
  $gitcmd checkout ${branches[i]}
  echo "$gitcmd checkout ${branches[i]}"

  deploy_cmd="mvn --batch-mode -Pjenkins deploy -DskipTests -Ddocker.skip-build=true -Ddocker.skip-test=true"
  deploy_cmd+=" -DaltDeploymentRepository=confluent-artifactory-central::default::s3://staging-ksqldb-maven/maven"
  deploy_cmd+=" -DrepositoryId=confluent-artifactory-central"
  deploy_cmd+=" -DnexusUrl=s3://staging-ksqldb-maven/maven"
  echo $deploy_cmd
  eval $deploy_cmd

done

# git clone each repo in list

# checkout CORRECT stabilization branch for each repo

# run mvn deploy on each branch




ksql_ver=$1
cc_spec_ksql_branch=$2
working_dir=$3
gitcmd="git --git-dir=./cc-spec-ksql/.git --work-tree=./cc-spec-ksql"
cc_spec_ksql_dir="./cc-spec-ksql"

git clone git@github.com:confluentinc/cc-spec-ksql.git $cc_spec_ksql_dir
$gitcmd checkout $cc_spec_ksql_branch

# get the current version
devel_ksql_ver=$(grep CurrentImage $cc_spec_ksql_dir/plugins/ksql/ksql_image.go | sed 's/.* = //')

# test if we should update or not
should_promote=$(python3 $working_dir/scripts/should-promote.py $ksql_ver $devel_ksql_ver)

# update if required
if [ -n "$FORCE_PROMOTE" ] || [ "$should_promote" == "yes" ]; then
	echo "Promoting from $devel_ksql_ver to $ksql_ver"
	bash $working_dir/scripts/generate-ksql-image-go.sh $ksql_ver > $cc_spec_ksql_dir/plugins/ksql/ksql_image.go
	$gitcmd commit -am "bump ksqldb image to $ksql_ver"
	$gitcmd push origin $cc_spec_ksql_branch
else
	echo "Skip promoting due to version mismatch $ksql_ver $devel_ksql_ver"
fi