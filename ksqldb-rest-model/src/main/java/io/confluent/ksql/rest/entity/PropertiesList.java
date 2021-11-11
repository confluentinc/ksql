/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.entity;


import static io.confluent.ksql.util.KsqlConfig.CONNECT_URL_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.CONNECT_WORKER_CONFIG_FILE_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACCESS_VALIDATOR_AUTO;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACCESS_VALIDATOR_OFF;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACCESS_VALIDATOR_ON;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_AUTH_CACHE_EXPIRY_TIME_SECS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_AUTH_CACHE_MAX_ENTRIES;
import static io.confluent.ksql.util.KsqlConfig.KSQL_COLLECT_UDF_METRICS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_CREATE_OR_REPLACE_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_CUSTOM_METRICS_EXTENSION;
import static io.confluent.ksql.util.KsqlConfig.KSQL_CUSTOM_METRICS_TAGS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ENABLE_ACCESS_VALIDATOR;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ENABLE_UDFS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ERROR_CLASSIFIER_REGEX_PREFIX;
import static io.confluent.ksql.util.KsqlConfig.KSQL_EXT_DIR;
import static io.confluent.ksql.util.KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_HIDDEN_TOPICS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_INSERT_INTO_VALUES_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.KSQL_LAMBDAS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION;
import static io.confluent.ksql.util.KsqlConfig.KSQL_NESTED_ERROR_HANDLING_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST;
import static io.confluent.ksql.util.KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_ERROR_MAX_QUEUE_SIZE;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_INTERPRETER_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_QPS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_METRICS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_RANGE_SCAN_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_LATEST_RESET_AGE_MS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_MS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_NODE_CONTINUITY;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_STREAM_PULL_QUERY_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_READONLY_TOPICS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SECURITY_EXTENSION_CLASS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SERVICE_ID_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SUPPRESS_BUFFER_SIZE_BYTES;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SUPPRESS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_TIMESTAMP_THROW_ON_INVALID;
import static io.confluent.ksql.util.KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING;
import static io.confluent.ksql.util.KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_TRANSIENT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE;
import static io.confluent.ksql.util.KsqlConfig.KSQL_WRAP_SINGLE_VALUES;
import static io.confluent.ksql.util.KsqlConfig.METRIC_REPORTER_CLASSES_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CHECK_CRCS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_RACK_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_INSTANCE_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.METADATA_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RECEIVE_BUFFER_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SECURITY_PROVIDERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SEND_BUFFER_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.METADATA_MAX_IDLE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.PARTITIONER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.MAX_TASK_IDLE_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.POLL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TASK_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.UPGRADE_FROM_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.WINDOW_SIZE_MS_CONFIG;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PropertiesList extends KsqlEntity {
  private static final List<String> InternalPropertiesList = ImmutableList.of(
      CONNECT_URL_PROPERTY,
      CONNECT_WORKER_CONFIG_FILE_PROPERTY,
      KSQL_ACCESS_VALIDATOR_AUTO,
      KSQL_ACCESS_VALIDATOR_OFF,
      KSQL_ACCESS_VALIDATOR_ON,
      KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG,
      KSQL_AUTH_CACHE_EXPIRY_TIME_SECS,
      KSQL_AUTH_CACHE_MAX_ENTRIES,
      KSQL_COLLECT_UDF_METRICS,
      KSQL_CREATE_OR_REPLACE_ENABLED,
      KSQL_CUSTOM_METRICS_TAGS,
      KSQL_ENABLE_ACCESS_VALIDATOR,
      KSQL_ENABLE_UDFS,
      KSQL_ERROR_CLASSIFIER_REGEX_PREFIX,
      KSQL_EXT_DIR,
      KSQL_HIDDEN_TOPICS_CONFIG,
      KSQL_INSERT_INTO_VALUES_ENABLED,
      KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS_PROPERTY,
      KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY,
      KSQL_LAMBDAS_ENABLED,
      KSQL_METASTORE_BACKUP_LOCATION,
      KSQL_PROPERTIES_OVERRIDES_DENYLIST,
      KSQL_PULL_QUERIES_ENABLE_CONFIG,
      KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE,
      KSQL_QUERYANONYMIZER_ENABLED,
      KSQL_QUERY_PULL_ENABLE_STANDBY_READS,
      KSQL_QUERY_PULL_INTERPRETER_ENABLED,
      KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG,
      KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_CONFIG,
      KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG,
      KSQL_QUERY_PULL_MAX_QPS_CONFIG,
      KSQL_QUERY_PULL_METRICS_ENABLED,
      KSQL_QUERY_PULL_RANGE_SCAN_ENABLED,
      KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_CONFIG,
      KSQL_QUERY_PULL_TABLE_SCAN_ENABLED,
      KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG,
      KSQL_QUERY_PUSH_V2_ENABLED,
      KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED,
      KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG,
      KSQL_QUERY_PUSH_V2_NEW_NODE_CONTINUITY,
      KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED,
      KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS,
      KSQL_QUERY_STREAM_PULL_QUERY_ENABLED,
      KSQL_READONLY_TOPICS_CONFIG,
      KSQL_ROWPARTITION_ROWOFFSET_ENABLED,
      KSQL_SECURITY_EXTENSION_CLASS,
      KSQL_SERVICE_ID_CONFIG,
      KSQL_SHARED_RUNTIME_ENABLED,
      KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG,
      KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED,
      KSQL_SUPPRESS_BUFFER_SIZE_BYTES,
      KSQL_SUPPRESS_ENABLED,
      KSQL_UDF_SECURITY_MANAGER_ENABLED,
      KSQL_VARIABLE_SUBSTITUTION_ENABLE,
      METRIC_REPORTER_CLASSES_CONFIG,
      SCHEMA_REGISTRY_URL_PROPERTY,
      KSQL_HEADERS_COLUMNS_ENABLED
  );

  private static final List<String> QueryLevelPropertyList = ImmutableList.of(
      KSQL_STRING_CASE_CONFIG_TOGGLE,
      KSQL_NESTED_ERROR_HANDLING_CONFIG,
      KSQL_QUERY_ERROR_MAX_QUEUE_SIZE,
      KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS,
      KSQL_QUERY_RETRY_BACKOFF_MAX_MS,
      KSQL_TIMESTAMP_THROW_ON_INVALID,
      FAIL_ON_DESERIALIZATION_ERROR_CONFIG
  );

  public static final List<String> EditablePropertyList = ImmutableList.of(
      KSQL_STRING_CASE_CONFIG_TOGGLE,
      KSQL_NESTED_ERROR_HANDLING_CONFIG,
      KSQL_QUERY_ERROR_MAX_QUEUE_SIZE,
      KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS,
      KSQL_QUERY_RETRY_BACKOFF_MAX_MS,
      KSQL_TIMESTAMP_THROW_ON_INVALID,
      FAIL_ON_DESERIALIZATION_ERROR_CONFIG,
      KSQL_SERVICE_ID_CONFIG,
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG,
      KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG,
      SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY,
      KSQL_DEFAULT_KEY_FORMAT_CONFIG,
      KSQL_DEFAULT_VALUE_FORMAT_CONFIG,
      KSQL_WRAP_SINGLE_VALUES,
      KSQL_CUSTOM_METRICS_EXTENSION,
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG,
      KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_MS,
      KSQL_QUERY_PUSH_V2_LATEST_RESET_AGE_MS,
      KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING,
      KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_TRANSIENT,
      GROUP_ID_CONFIG,
      GROUP_INSTANCE_ID_CONFIG,
      MAX_POLL_RECORDS_CONFIG,
      MAX_POLL_INTERVAL_MS_CONFIG,
      SESSION_TIMEOUT_MS_CONFIG,
      HEARTBEAT_INTERVAL_MS_CONFIG,
      BOOTSTRAP_SERVERS_CONFIG,
      CLIENT_DNS_LOOKUP_CONFIG,
      ENABLE_AUTO_COMMIT_CONFIG,
      AUTO_COMMIT_INTERVAL_MS_CONFIG,
      PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
      AUTO_OFFSET_RESET_CONFIG,
      FETCH_MIN_BYTES_CONFIG,
      FETCH_MAX_BYTES_CONFIG,
      FETCH_MAX_WAIT_MS_CONFIG,
      METADATA_MAX_AGE_CONFIG,
      MAX_PARTITION_FETCH_BYTES_CONFIG,
      SEND_BUFFER_CONFIG,
      RECEIVE_BUFFER_CONFIG,
      CLIENT_ID_CONFIG,
      CLIENT_RACK_CONFIG,
      RECONNECT_BACKOFF_MS_CONFIG,
      RECONNECT_BACKOFF_MAX_MS_CONFIG,
      RETRY_BACKOFF_MS_CONFIG,
      METRICS_SAMPLE_WINDOW_MS_CONFIG,
      METRICS_NUM_SAMPLES_CONFIG,
      METRICS_RECORDING_LEVEL_CONFIG,
      CHECK_CRCS_CONFIG,
      KEY_DESERIALIZER_CLASS_CONFIG,
      VALUE_DESERIALIZER_CLASS_CONFIG,
      SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
      SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
      CONNECTIONS_MAX_IDLE_MS_CONFIG,
      REQUEST_TIMEOUT_MS_CONFIG,
      DEFAULT_API_TIMEOUT_MS_CONFIG,
      INTERCEPTOR_CLASSES_CONFIG,
      EXCLUDE_INTERNAL_TOPICS_CONFIG,
      ISOLATION_LEVEL_CONFIG,
      ALLOW_AUTO_CREATE_TOPICS_CONFIG,
      SECURITY_PROVIDERS_CONFIG,
      METADATA_MAX_IDLE_CONFIG,
      BATCH_SIZE_CONFIG,
      ACKS_CONFIG,
      LINGER_MS_CONFIG,
      DELIVERY_TIMEOUT_MS_CONFIG,
      MAX_REQUEST_SIZE_CONFIG,
      MAX_BLOCK_MS_CONFIG,
      BUFFER_MEMORY_CONFIG,
      COMPRESSION_TYPE_CONFIG,
      RETRIES_CONFIG,
      KEY_SERIALIZER_CLASS_CONFIG,
      VALUE_SERIALIZER_CLASS_CONFIG,
      PARTITIONER_CLASS_CONFIG,
      ENABLE_IDEMPOTENCE_CONFIG,
      TRANSACTION_TIMEOUT_CONFIG,
      TRANSACTIONAL_ID_CONFIG,
      ACCEPTABLE_RECOVERY_LAG_CONFIG,
      APPLICATION_ID_CONFIG,
      APPLICATION_SERVER_CONFIG,
      BUILT_IN_METRICS_VERSION_CONFIG,
      CACHE_MAX_BYTES_BUFFERING_CONFIG,
      COMMIT_INTERVAL_MS_CONFIG,
      DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
      DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
      DEFAULT_KEY_SERDE_CLASS_CONFIG,
      DEFAULT_VALUE_SERDE_CLASS_CONFIG,
      DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
      MAX_TASK_IDLE_MS_CONFIG,
      MAX_WARMUP_REPLICAS_CONFIG,
      NUM_STANDBY_REPLICAS_CONFIG,
      NUM_STREAM_THREADS_CONFIG,
      POLL_MS_CONFIG,
      PROBING_REBALANCE_INTERVAL_MS_CONFIG,
      PROCESSING_GUARANTEE_CONFIG,
      REPLICATION_FACTOR_CONFIG,
      ROCKSDB_CONFIG_SETTER_CLASS_CONFIG,
      SECURITY_PROTOCOL_CONFIG,
      STATE_CLEANUP_DELAY_MS_CONFIG,
      STATE_DIR_CONFIG,
      TASK_TIMEOUT_MS_CONFIG,
      TOPOLOGY_OPTIMIZATION_CONFIG,
      WINDOW_SIZE_MS_CONFIG,
      UPGRADE_FROM_CONFIG
  );

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Property {
    private final String name;
    private final String scope;
    private final String value;

    @JsonProperty("internal")
    private final boolean internal;

    @JsonProperty("level")
    private final String level;

    @JsonCreator
    public Property(
        @JsonProperty("name") final String name,
        @JsonProperty("scope") final String scope,
        @JsonProperty("value") final String value
    ) {
      this.name = name;
      this.scope = scope;
      this.value = value;
      this.internal = PropertiesList.InternalPropertiesList.contains(name);
      this.level = PropertiesList.QueryLevelPropertyList.contains(name) ? "QUERY" : "SERVER";
    }

    public String getLevel() {
      return level;
    }

    public boolean getInternal() {
      return internal;
    }

    public String getName() {
      return name;
    }

    public String getScope() {
      return scope;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(final Object object) {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      final Property that = (Property) object;
      return Objects.equals(name, that.name)
          && Objects.equals(scope, that.scope)
          && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, scope, value);
    }

    @Override
    public String toString() {
      return "Property{"
          + "name='" + name + '\''
          + ", scope='" + scope + '\''
          + ", value='" + value + '\''
          + '}';
    }
  }

  private final ImmutableList<Property> properties;
  private final ImmutableList<String> overwrittenProperties;
  private final ImmutableList<String> defaultProperties;

  @JsonCreator
  public PropertiesList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("properties") final List<Property> properties,
      @JsonProperty("overwrittenProperties") final List<String> overwrittenProperties,
      @JsonProperty("defaultProperties") final List<String> defaultProperties
  ) {
    super(statementText);
    this.properties = ImmutableList.copyOf(properties);
    this.overwrittenProperties = ImmutableList.copyOf(overwrittenProperties);
    this.defaultProperties = ImmutableList.copyOf(defaultProperties);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "properties is ImmutableList")
  public List<Property> getProperties() {
    return properties;
  }

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "overwrittenProperties is ImmutableList"
  )
  public List<String> getOverwrittenProperties() {
    return overwrittenProperties;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "defaultProperties is ImmutableList")
  public List<String> getDefaultProperties() {
    return defaultProperties;
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof PropertiesList
        && Objects.equals(properties, ((PropertiesList)o).properties)
        && Objects.equals(overwrittenProperties, ((PropertiesList)o).overwrittenProperties)
        && Objects.equals(defaultProperties, ((PropertiesList)o).defaultProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, overwrittenProperties, defaultProperties);
  }
}
