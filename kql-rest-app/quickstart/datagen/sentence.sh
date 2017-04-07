#! /usr/bin/env bash

set -e

BASE_DIR=$(dirname "$0")
KQL_DIR="$BASE_DIR/../../.."

if ! [ -f "$KQL_DIR/kql-examples/target/kql-examples" ]; then
  echo 1>&2 "Failed to locate kql-examples script. Perhaps you forgot to build?"
  exit 1
fi

unset DELETE_NOUNS
unset DELETE_ADJECTIVES

if ! [ -f nouns-list.json ]; then
  ln -s "$BASE_DIR/nouns-list.json" nouns-list.json
  DELETE_NOUNS=true
fi

if ! [ -f adjectives-list.json ]; then
  ln -s "$BASE_DIR/adjectives-list.json" adjectives-list.json
  DELETE_ADJECTIVES=true
fi

"$KQL_DIR/kql-examples/target/kql-examples" \
  schema="$BASE_DIR/sentence_schema.avro"   \
  topic=sentence_kafka_topic_json           \
  key=id                                    \
  format=json                               \
  iterations="${1:-1}"

if [ "$DELETE_NOUNS" = "true" ]; then
  rm nouns-list.json
fi

if [ "$DELETE_ADJECTIVES" = "true" ]; then
  rm adjectives-list.json
fi
