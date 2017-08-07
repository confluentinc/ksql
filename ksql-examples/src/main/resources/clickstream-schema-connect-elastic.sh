#!/bin/bash

#` -- [ '37.145.8.144' | '-' | '-' | '02/Aug/2017:12:39:55 -0700' | 1501702795917 | 'GET /site/login.html HTTP/1.1' | '404' | '4196' | '-' | 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)' ])
# -- json
# -- {"remote_user":"-","request":"GET /site/login.html HTTP/1.1","referrer":"-","agent":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)","bytes":"4196","ip":"37.145.8.144","time":"02/Aug/2017:12:39:55 -0700","userid":"-","_time":1501702795917,"status":"404"}

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

  curl -X "POST" "http://localhost:8083/connectors/" \
       -H "Content-Type: application/json" \
       -d $'{
    "name": "es_sink_CLICKSTREAM_CODES_TS",
    "config": {
      "schema.ignore": "true",
      "topics": "CLICKSTREAM_CODES_TS",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter.schemas.enable": false,
      "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
      "key.ignore": "true",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "type.name": "type.name=kafkaconnect",
      "topic.index.map": "CLICKSTREAM_CODES_TS:clickstream_status_codes_ts",
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
   "name": "es_sink_ERRORS_PER_MIN_10_TS",
   "config": {
     "schema.ignore": "true",
     "topics": "ERRORS_PER_MIN_10_TS",
     "key.converter": "org.apache.kafka.connect.storage.StringConverter",
     "value.converter.schemas.enable": false,
     "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
     "key.ignore": "true",
     "value.converter": "org.apache.kafka.connect.json.JsonConverter",
     "type.name": "type.name=kafkaconnect",
     "topic.index.map": "ERRORS_PER_MIN_10_TS:errors_per_min_10_ts",
     "connection.url": "http://localhost:9200"
   }
 }'

