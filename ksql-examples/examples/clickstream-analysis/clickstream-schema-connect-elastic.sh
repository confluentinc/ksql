#!/bin/bash

curl -X "POST" "http://localhost:8083/connectors/" \
     -H "Content-Type: application/json" \
     -d $'{
  "name": "es_sink_CLICK_USER_SESSIONS_TS",
  "config": {
    "schema.ignore": "true",
    "topics": "CLICK_USER_SESSIONS_TS",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.schemas.enable": false,
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "key.ignore": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "type.name": "type.name=kafkaconnect",
    "topic.index.map": "CLICK_USER_SESSIONS_TS:click_user_sessions_ts",
    "connection.url": "http://localhost:9200"
  }
}'


curl -X "POST" "http://localhost:8083/connectors/" \
     -H "Content-Type: application/json" \
     -d $'{
  "name": "es_sink_EVENTS_PER_MIN_TS",
  "config": {
    "schema.ignore": "true",
    "topics": "EVENTS_PER_MIN_TS",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.schemas.enable": false,
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "key.ignore": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "type.name": "type.name=kafkaconnect",
    "topic.index.map": "EVENTS_PER_MIN_TS:events_per_min_ts",
    "connection.url": "http://localhost:9200"
  }
}'


curl -X "POST" "http://localhost:8083/connectors/" \
     -H "Content-Type: application/json" \
     -d $'{
  "name": "es_sink_EVENTS_PER_MIN_MAX_AVG_TS",
  "config": {
    "schema.ignore": "true",
    "topics": "EVENTS_PER_MIN_MAX_AVG_TS",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.schemas.enable": false,
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "key.ignore": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "type.name": "type.name=kafkaconnect",
    "topic.index.map": "EVENTS_PER_MIN_MAX_AVG_TS:events_per_min_max_avg_ts",
    "connection.url": "http://localhost:9200"
  }
}'


 curl -X "POST" "http://localhost:8083/connectors/" \
      -H "Content-Type: application/json" \
      -d $'{
   "name": "es_sink_PAGES_PER_MIN_TS",
   "config": {
     "schema.ignore": "true",
     "topics": "PAGES_PER_MIN_TS",
     "key.converter": "org.apache.kafka.connect.storage.StringConverter",
     "value.converter.schemas.enable": false,
     "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
     "key.ignore": "true",
     "value.converter": "org.apache.kafka.connect.json.JsonConverter",
     "type.name": "type.name=kafkaconnect",
     "topic.index.map": "PAGES_PER_MIN_TS:pages_per_min_ts",
     "connection.url": "http://localhost:9200"
   }
 }'
 
 curl -X "POST" "http://localhost:8083/connectors/" \
      -H "Content-Type: application/json" \
      -d $'{
   "name": "es_sink_ERRORS_PER_MIN_TS",
   "config": {
     "schema.ignore": "true",
     "topics": "ERRORS_PER_MIN_TS",
     "key.converter": "org.apache.kafka.connect.storage.StringConverter",
     "value.converter.schemas.enable": false,
     "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
     "key.ignore": "true",
     "value.converter": "org.apache.kafka.connect.json.JsonConverter",
     "type.name": "type.name=kafkaconnect",
     "topic.index.map": "ERRORS_PER_MIN_TS:errors_per_min_ts",
     "connection.url": "http://localhost:9200"
   }
 }'

 curl -X "POST" "http://localhost:8083/connectors/" \
       -H "Content-Type: application/json" \
       -d $'{
    "name": "es_sink_ERRORS_PER_MIN_ALERT_TS",
    "config": {
      "schema.ignore": "true",
      "topics": "ERRORS_PER_MIN_ALERT_TS",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter.schemas.enable": false,
      "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
      "key.ignore": "true",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "type.name": "type.name=kafkaconnect",
      "topic.index.map": "ERRORS_PER_MIN_ALERT_TS:errors_per_min_alert_ts",
      "connection.url": "http://localhost:9200"
    }
  }'

  curl -X "POST" "http://localhost:8083/connectors/" \
       -H "Content-Type: application/json" \
       -d $'{
    "name": "es_sink_ENRICHED_ERROR_CODES_TS",
    "config": {
      "schema.ignore": "true",
      "topics": "ENRICHED_ERROR_CODES_TS",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter.schemas.enable": false,
      "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
      "key.ignore": "true",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "type.name": "type.name=kafkaconnect",
      "topic.index.map": "ENRICHED_ERROR_CODES_TS:enriched_error_codes_ts",
      "connection.url": "http://localhost:9200"
    }
  }'


