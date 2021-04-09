#! /bin/bash

set -e

increment_failures() {
  local message=$1

  echo "******************************************************"
  echo "* SMOKE TEST FAILED: $message"
  echo "*"
  echo "* Backtrace:"

  # http://wiki.bash-hackers.org/commands/builtin/caller
  local frame=0
  # shellcheck disable=SC2207
  while caller_info=($(caller $frame)); do
    echo "* $frame) in ${caller_info[1]}; from ${caller_info[2]}:${caller_info[0]}"

    ((frame = frame + 1))
  done

  echo
  echo "******************************************************"
  echo "              SMOKE TESTS FAILED"
  echo "******************************************************"
  echo
  echo "Something went wrong with the tests."
  echo
  echo "See the above output for details about individual test"
  echo "failures."
  echo

  exit 1
}

# Implies zero or positive
is_natural() {
  [[ -n "${1}" ]] && [[ "${1}" =~ ^[0-9]+$ ]]
}

timed_wait() {
  set +e
  local timeout_s="${1}"
  shift
  local condition=$*
  is_natural "${timeout_s}" ||
    increment_failures "time out is not set properly"
  status=1
  while [[ ${status} -ne 0 && "${timeout_s}" -gt 0 ]]; do
    eval "${condition}"
    status=$?
    ((timeout_s = timeout_s - 1))
    sleep 1
  done
  set -e
  if [[ ${timeout_s} -le 0 ]]; then
    return "${status}"
  fi
  return "${status}"
}

DEFAULT_TIMED_OUT=30

echo "Starting Zookeeper"
/usr/bin/zookeeper-server-start /etc/kafka/zookeeper.properties  > zookeeper.out &
timed_wait $DEFAULT_TIMED_OUT "grep -qe 'binding to port' zookeeper.out" ||
    increment_failures "start zk fail"

echo "Starting Kafka"
/usr/bin/kafka-server-start /etc/kafka/server.properties > kafka.out &
timed_wait $DEFAULT_TIMED_OUT "grep -qe 'started (kafka.server.KafkaServer)' kafka.out" ||
    increment_failures "start kafka fail"

echo "Starting ksqlDB"
/usr/bin/ksql-server-start /etc/ksqldb/ksql-server.properties  > ksqldb.out &
timed_wait $DEFAULT_TIMED_OUT \
    "curl -sf http://localhost:8088/info" ||
    increment_failures "Unable to curl ksql server at http://localhost:8088/info"

CREATE_STREAM=$(cat <<-EOM
CREATE STREAM reviews
  (item_id BIGINT, user_id BIGINT, comment string) 
  WITH (KAFKA_TOPIC='reviews',
        VALUE_FORMAT='json',
	partitions=1);
EOM
)
CREATE_STREAM=$(echo '{"ksql": "'"$CREATE_STREAM"'", "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"}}' | tr '\n' ' ')

curl -X "POST" "http://localhost:8088/ksql" \
     -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
     -d "$CREATE_STREAM" ||
    increment_failures "Unable to do create stream"

CREATE_TABLE=$(cat <<-EOM
CREATE TABLE reviews_by_item AS
   SELECT item_id, latest_by_offset(comment) FROM reviews group by item_id;
EOM
)
CREATE_TABLE=$(echo '{"ksql": "'"$CREATE_TABLE"'", "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"}}' | tr '\n' ' ')

curl -X "POST" "http://localhost:8088/ksql" \
     -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
     -d "$CREATE_TABLE" ||
    increment_failures "Unable to do create table"

INSERT_DATA=$(cat <<-EOM
INSERT INTO reviews VALUES (1, 100, 'Really cool');
INSERT INTO reviews VALUES (2, 100, 'Meh :-(');
INSERT INTO reviews VALUES (1, 101, 'Broke immediately!');
INSERT INTO reviews VALUES (2, 101, 'Not bad');
EOM
)
INSERT_DATA=$(echo '{"ksql": "'"$INSERT_DATA"'", "streamsProperties": {}}' | tr '\n' ' ')

curl -X "POST" "http://localhost:8088/ksql" \
     -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
     -d "$INSERT_DATA" ||
    increment_failures "Unable to insert data"


RESULT=$(curl -X "POST" "http://localhost:8088/query" \
     -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
     -d '{"ksql": "SELECT * FROM reviews EMIT CHANGES limit 4;", "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"}}' ||
    increment_failures "Unable do push query")
NUM_ROWS=$(echo "$RESULT" | jq . | grep row | wc | awk '{print $1}')

if [ "$NUM_ROWS" -ne 4 ]; then
    increment_failures "Failed to find rows $RESULT"
else
    echo "Found expected rows $RESULT"
fi

do_pull_query() {
  RESULT=$(curl -X "POST" "http://localhost:8088/query" \
       -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
       -d '{"ksql": "SELECT * FROM reviews_by_item where item_id = 1;", "streamsProperties": {}}' ||
      increment_failures "Unable do pull query")
  NUM_ROWS=$(echo "$RESULT" | jq . | grep row | wc | awk '{print $1}')

  if [ "$NUM_ROWS" -ne 1 ]; then
      echo "Pull Query: Failed to find row $RESULT" 
      return 1
  else
      echo "Pull Query: Found expected rows $RESULT"
      return 0
  fi
}

timed_wait $DEFAULT_TIMED_OUT "do_pull_query" ||
    increment_failures "Failed to find pull query row"
