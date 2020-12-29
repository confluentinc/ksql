#!/usr/bin/env bash
#
# Copyright [2017 - 2020] Confluent Inc.
#

# Extract AWS credential using its assigned IAM role.
#
# This script is meant to be sourced by a parent script, so from your parent script:
# . jenkins-common/scripts/aws/extract-iam-credential.sh

[ -o xtrace ] && restore_trace='set -x' || restore_trace='set +x'
set +x


if [[ ${AWS_ACCESS_KEY_ID:-} && ${AWS_ACCESS_KEY:-} && ${AWS_SECRET_ACCESS_KEY:-} && ${AWS_SECRET_KEY:-} && ! ${AWS_SESSION_TOKEN:-} ]]; then
  echo "Using existing AWS credential from env"
else
  echo "Extracting AWS credential based on IAM role"

  if [[ ! "${AWS_IAM:-}" ]];then
      # Get the iam rolename associated with this ec2 instance. The curl command actually fetches the iam instance profile associated with this ec2 node,
      # and what we really need is the iam role name, so we rely on the convention that
      # iam instance profile name and iam role name are the same
      export AWS_IAM=$(curl -s http://169.254.169.254/latest/meta-data/iam/info | grep InstanceProfileArn | cut -d '"' -f 4 | cut -d '/' -f 2)
  fi

  export AWS_ACCESS_KEY_ID=`curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/$AWS_IAM | grep AccessKeyId | awk -F\" '{ print $4 }'`
  export AWS_ACCESS_KEY="$AWS_ACCESS_KEY_ID"
  export AWS_SECRET_KEY=`curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/$AWS_IAM | grep SecretAccessKey | awk -F\" '{ print $4 }'`
  export AWS_SECRET_ACCESS_KEY="$AWS_SECRET_KEY"
  export AWS_SESSION_TOKEN=`curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/$AWS_IAM | grep Token | awk -F\" '{ print $4 }'`

  if [ -z "$AWS_ACCESS_KEY" ]; then
      echo "Failed to populate environment variables AWS_ACCESS_KEY, AWS_SECRET_KEY, and AWS_SESSION_TOKEN."
      echo "AWS_IAM is currently $AWS_IAM. Double-check that this is correct. If not, update AWS_IAM env in aws.sh"
      exit 1
  fi

fi

export AWS_DEFAULT_REGION=us-west-2

eval "$restore_trace"
