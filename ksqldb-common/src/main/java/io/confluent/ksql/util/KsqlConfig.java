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

package io.confluent.ksql.util;

import static io.confluent.ksql.configdef.ConfigValidators.zeroOrPositive;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.config.ConfigItem;
import io.confluent.ksql.config.KsqlConfigResolver;
import io.confluent.ksql.configdef.ConfigValidators;
import io.confluent.ksql.errors.LogMetricAndContinueExceptionHandler;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.model.SemanticVersion;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EffectivelyImmutable
public class KsqlConfig extends AbstractConfig {
  private static final Logger LOG = LoggerFactory.getLogger(KsqlConfig.class);

  public static final String KSQL_CONFIG_PROPERTY_PREFIX = "ksql.";

  public static final String KSQL_FUNCTIONS_PROPERTY_PREFIX =
      KSQL_CONFIG_PROPERTY_PREFIX + "functions.";

  static final String KSQ_FUNCTIONS_GLOBAL_PROPERTY_PREFIX =
      KSQL_FUNCTIONS_PROPERTY_PREFIX + "_global_.";

  public static final String KSQL_DEPLOYMENT_TYPE_CONFIG =
      KSQL_CONFIG_PROPERTY_PREFIX + "deployment.type";

  public enum DeploymentType {
    selfManaged,
    confluent
  }

  public static final String KSQL_DEPLOYMENT_TYPE_DOC =
      "The type of deployment for ksql. Value must be one of "
              + Arrays.asList(DeploymentType.values());

  public static final String METRIC_REPORTER_CLASSES_CONFIG =
      CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

  public static final String METRIC_REPORTER_CLASSES_DOC =
      CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC;

  private static final String TELEMETRY_REPORTER_CLASS =
          "io.confluent.telemetry.reporter.TelemetryReporter";

  private static final String TELEMETRY_PREFIX = "confluent.telemetry";
  private static final Set<String> REPORTER_CONFIGS_PREFIXES =
      ImmutableSet.of(
          TELEMETRY_PREFIX,
          CommonClientConfigs.METRICS_CONTEXT_PREFIX
      );

  public static final String KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY = "ksql.internal.topic.replicas";

  public static final String KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS_PROPERTY =
      "ksql.internal.topic.min.insync.replicas";

  public static final String KSQL_INTERNAL_METRIC_COLLECTORS_CONFIG =
      "ksql.internal.metric.collectors";

  public static final String KSQL_INTERNAL_METRICS_CONFIG =
      "ksql.internal.metrics";

  public static final String KSQL_INTERNAL_STREAMS_ERROR_COLLECTOR_CONFIG =
      "ksql.internal.streams.error.collector";

  public static final String KSQL_SCHEMA_REGISTRY_PREFIX = "ksql.schema.registry.";

  public static final String SCHEMA_REGISTRY_URL_PROPERTY = "ksql.schema.registry.url";

  public static final String KSQL_CONNECT_PREFIX = "ksql.connect.";

  public static final String CONNECT_URL_PROPERTY = KSQL_CONNECT_PREFIX + "url";

  public static final String CONNECT_WORKER_CONFIG_FILE_PROPERTY =
      KSQL_CONNECT_PREFIX + "worker.config";

  public static final String CONNECT_BASIC_AUTH_CREDENTIALS_SOURCE_PROPERTY =
      KSQL_CONNECT_PREFIX + "basic.auth.credentials.source";
  public static final String BASIC_AUTH_CREDENTIALS_SOURCE_NONE = "NONE";
  public static final String BASIC_AUTH_CREDENTIALS_SOURCE_FILE = "FILE";
  private static final ConfigDef.ValidString BASIC_AUTH_CREDENTIALS_SOURCE_VALIDATOR =
      ConfigDef.ValidString.in(
          BASIC_AUTH_CREDENTIALS_SOURCE_NONE,
          BASIC_AUTH_CREDENTIALS_SOURCE_FILE
      );
  public static final String BASIC_AUTH_CREDENTIALS_USERNAME = "username";
  public static final String BASIC_AUTH_CREDENTIALS_PASSWORD = "password";

  public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG =
      SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
  public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_HTTPS = "https";
  public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_NONE = "none";

  public static final String CONNECT_BASIC_AUTH_CREDENTIALS_FILE_PROPERTY =
      KSQL_CONNECT_PREFIX + "basic.auth.credentials.file";
  public static final String CONNECT_BASIC_AUTH_CREDENTIALS_RELOAD_PROPERTY =
      KSQL_CONNECT_PREFIX + "basic.auth.credentials.reload";

  public static final String CONNECT_REQUEST_TIMEOUT_MS =
      KSQL_CONNECT_PREFIX + "request.timeout.ms";
  public static final Long CONNECT_REQUEST_TIMEOUT_DEFAULT = 5_000L;
  private static final String CONNECT_REQUEST_TIMEOUT_MS_DOC =
      "Timeout, in milliseconds, used for each of the connection timeout and request timeout "
          + "for connector requests issued by ksqlDB.";

  public static final String CONNECT_REQUEST_HEADERS_PLUGIN =
      KSQL_CONNECT_PREFIX + "request.headers.plugin";
  private static final String CONNECT_REQUEST_HEADERS_PLUGIN_DOC =
      "Custom extension to allow for more fine-grained control of connector requests made by "
          + "ksqlDB. Extensions should implement the ConnectRequestHeadersExtension interface.";

  public static final String KSQL_ENABLE_UDFS = "ksql.udfs.enabled";

  public static final String KSQL_EXT_DIR = "ksql.extension.dir";

  public static final String SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY =
      "ksql.sink.window.change.log.additional.retention";

  public static final String
      FAIL_ON_DESERIALIZATION_ERROR_CONFIG = "ksql.fail.on.deserialization.error";

  public static final String FAIL_ON_PRODUCTION_ERROR_CONFIG = "ksql.fail.on.production.error";

  public static final String
      KSQL_SERVICE_ID_CONFIG = "ksql.service.id";
  public static final String
      KSQL_SERVICE_ID_DEFAULT = "default_";

  public static final String
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG = "ksql.persistent.prefix";
  public static final String
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT = "query_";
  public static final String
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_DOC = "Prefixes persistent queries with this value.";

  public static final String
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG = "ksql.transient.prefix";
  public static final String
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT = "transient_";

  public static final String
      KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG = "ksql.output.topic.name.prefix";
  private static final String KSQL_OUTPUT_TOPIC_NAME_PREFIX_DOCS =
      "A prefix to add to any output topic names, where the statement does not include an explicit "
      + "topic name. E.g. given 'ksql.output.topic.name.prefix = \"thing-\"', then statement "
      + "'CREATE STREAM S AS ...' will create a topic 'thing-S', where as the statement "
      + "'CREATE STREAM S WITH(KAFKA_TOPIC = 'foo') AS ...' will create a topic 'foo'.";

  public static final String KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG =
      "ksql.query.persistent.active.limit";
  private static final int KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DEFAULT = Integer.MAX_VALUE;
  private static final String KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DOC =
      "An upper limit on the number of active, persistent queries that may be running at a time, "
      + "in interactive mode. Once this limit is reached, any further persistent queries will not "
      + "be accepted.";

  public static final String KSQL_DEFAULT_KEY_FORMAT_CONFIG = "ksql.persistence.default.format.key";
  private static final String KSQL_DEFAULT_KEY_FORMAT_DEFAULT = "KAFKA";
  private static final String KSQL_DEFAULT_KEY_FORMAT_DOC =
      "Key format that will be used by default if none is specified in the WITH properties of "
          + "CREATE STREAM/TABLE statements.";

  public static final String KSQL_DEFAULT_VALUE_FORMAT_CONFIG =
      "ksql.persistence.default.format.value";
  private static final String KSQL_DEFAULT_VALUE_FORMAT_DOC =
      "Value format that will be used by default if none is specified in the WITH properties of "
          + "CREATE STREAM/TABLE statements.";

  public static final String KSQL_WRAP_SINGLE_VALUES =
      "ksql.persistence.wrap.single.values";

  public static final String KSQL_QUERYANONYMIZER_ENABLED =
      "ksql.queryanonymizer.logs_enabled";
  private static final String KSQL_QUERYANONYMIZER_ENABLED_DOC =
      "This defines whether we log anonymized queries out of query logger or if we log them"
      + "in plain text. Defaults to true";
  public static final String KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE =
      "ksql.queryanonymizer.cluster_namespace";
  private static final String KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE_DOC =
      "Namespace used in the query anonymization process, representing cluster id and "
          + "organization id respectively. For example, 'clusterid.orgid'.";

  public static final String KSQL_CUSTOM_METRICS_TAGS = "ksql.metrics.tags.custom";
  private static final String KSQL_CUSTOM_METRICS_TAGS_DOC =
      "A list of tags to be included with emitted JMX metrics, formatted as a string of key:value "
      + "pairs separated by commas. For example, 'key1:value1,key2:value2'.";

  public static final String KSQL_CUSTOM_METRICS_EXTENSION = "ksql.metrics.extension";
  private static final String KSQL_CUSTOM_METRICS_EXTENSION_DOC =
      "Extension for supplying custom metrics to be emitted along with "
      + "the engine's default JMX metrics";

  public static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";
  public static final String DEFAULT_CONNECT_URL = "http://localhost:8083";

  public static final String KSQL_STREAMS_PREFIX = "ksql.streams.";

  public static final String KSQL_COLLECT_UDF_METRICS = "ksql.udf.collect.metrics";
  public static final String KSQL_UDF_SECURITY_MANAGER_ENABLED = "ksql.udf.enable.security.manager";

  public static final String KSQL_INSERT_INTO_VALUES_ENABLED = "ksql.insert.into.values.enabled";

  public static final String DEFAULT_EXT_DIR = "ext";

  public static final String KSQL_SECURITY_EXTENSION_CLASS = "ksql.security.extension.class";
  public static final String KSQL_SECURITY_EXTENSION_DEFAULT = null;
  public static final String KSQL_SECURITY_EXTENSION_DOC = "A KSQL security extension class that "
      + "provides authorization to KSQL servers.";

  public static final String KSQL_ENABLE_ACCESS_VALIDATOR = "ksql.access.validator.enable";
  public static final String KSQL_ACCESS_VALIDATOR_ON = "on";
  public static final String KSQL_ACCESS_VALIDATOR_OFF = "off";
  public static final String KSQL_ACCESS_VALIDATOR_AUTO = "auto";
  public static final String KSQL_ACCESS_VALIDATOR_DOC =
      "Config to enable/disable the topic access validator, which checks that KSQL can access "
          + "the involved topics before committing to execute a statement. Possible values are "
          + "\"on\", \"off\", and \"auto\". Setting to \"on\" enables the validator. Setting to "
          + "\"off\" disables the validator. If set to \"auto\", KSQL will attempt to discover "
          + "whether the Kafka cluster supports the required API, and enables the validator if "
          + "it does.";

  public static final String KSQL_PULL_QUERIES_ENABLE_CONFIG = "ksql.pull.queries.enable";
  public static final String KSQL_QUERY_PULL_ENABLE_DOC =
      "Config to enable or disable transient pull queries on a specific KSQL server.";
  public static final boolean KSQL_QUERY_PULL_ENABLE_DEFAULT = true;

  public static final String KSQL_QUERY_PULL_ENABLE_STANDBY_READS =
        "ksql.query.pull.enable.standby.reads";
  private static final String KSQL_QUERY_PULL_ENABLE_STANDBY_READS_DOC =
      "Config to enable/disable forwarding pull queries to standby hosts when the active is dead. "
          + "This means that stale values may be returned for these queries since standby hosts"
          + "receive updates from the changelog topic (to which the active writes to) "
          + "asynchronously. Turning on this configuration, effectively sacrifices "
          + "consistency for higher availability.  "
          + "Possible values are \"true\", \"false\". Setting to \"true\" guarantees high "
          + "availability for pull queries. If set to \"false\", pull queries will fail when"
          + "the active is dead and until a new active is elected. Default value is \"false\". "
          + "For using this functionality, the server must be configured with "
          + "to ksql.streams.num.standby.replicas >= 1";
  public static final boolean KSQL_QUERY_PULL_ENABLE_STANDBY_READS_DEFAULT = false;

  public static final String KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG =
      "ksql.query.pull.max.allowed.offset.lag";
  public static final Long KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_DEFAULT = Long.MAX_VALUE;
  private static final String KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_DOC =
      "Controls the maximum lag tolerated by a pull query against a table. This is applied to all "
          + "hosts storing it, both active and standbys included. This can be overridden per query "
          + "or set in the CLI. It's only enabled when lag.reporting.enable is true. "
          + "By default, any amount of lag is is allowed.";

  public static final String KSQL_QUERY_PULL_METRICS_ENABLED =
      "ksql.query.pull.metrics.enabled";
  public static final String KSQL_QUERY_PULL_METRICS_ENABLED_DOC =
      "Config to enable/disable collecting JMX metrics for pull queries.";

  public static final String KSQL_QUERY_PULL_MAX_QPS_CONFIG = "ksql.query.pull.max.qps";
  public static final Integer KSQL_QUERY_PULL_MAX_QPS_DEFAULT = Integer.MAX_VALUE;
  public static final String KSQL_QUERY_PULL_MAX_QPS_DOC = "The maximum qps allowed for pull "
      + "queries on this host. Once the limit is hit, queries will fail immediately";

  public static final String KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_CONFIG
      = "ksql.query.pull.max.concurrent.requests";
  public static final Integer KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_DEFAULT = Integer.MAX_VALUE;
  public static final String KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_DOC =
      "The maximum number of concurrent requests allowed for pull "
      + "queries on this host. Once the limit is hit, queries will fail immediately";

  public static final String KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG
      = "ksql.query.pull.max.hourly.bandwidth.megabytes";
  public static final Integer KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_DEFAULT
      = Integer.MAX_VALUE;
  public static final String KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_DOC
      = "The maximum amount of pull query bandwidth in megabytes allowed over"
      + " a period of one hour. Once the limit is hit, queries will fail immediately";

  public static final String KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG
      = "ksql.query.push.v2.max.hourly.bandwidth.megabytes";
  public static final Integer KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_DEFAULT
      = Integer.MAX_VALUE;
  public static final String KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_DOC
      = "The maximum amount of v2 push query bandwidth in megabytes allowed over"
      + " a period of one hour. Once the limit is hit, queries will fail immediately";

  public static final String KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG
      = "ksql.query.pull.thread.pool.size";
  public static final Integer KSQL_QUERY_PULL_THREAD_POOL_SIZE_DEFAULT = 50;
  public static final String KSQL_QUERY_PULL_THREAD_POOL_SIZE_DOC =
      "Size of thread pool used for coordinating pull queries";

  public static final String KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_CONFIG
      = "ksql.query.pull.router.thread.pool.size";
  public static final Integer KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_DEFAULT = 50;
  public static final String KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_DOC =
      "Size of thread pool used for routing pull queries";

  public static final String KSQL_QUERY_PULL_TABLE_SCAN_ENABLED
      = "ksql.query.pull.table.scan.enabled";
  public static final String KSQL_QUERY_PULL_TABLE_SCAN_ENABLED_DOC =
      "Config to enable pull queries that scan over the data";
  public static final boolean KSQL_QUERY_PULL_TABLE_SCAN_ENABLED_DEFAULT = true;

  public static final String KSQL_QUERY_STREAM_PULL_QUERY_ENABLED 
      = "ksql.query.pull.stream.enabled";
  public static final String KSQL_QUERY_STREAM_PULL_QUERY_ENABLED_DOC =
      "Config to enable pull queries on streams";
  public static final boolean KSQL_QUERY_STREAM_PULL_QUERY_ENABLED_DEFAULT = true;

  public static final String KSQL_QUERY_PULL_RANGE_SCAN_ENABLED
      = "ksql.query.pull.range.scan.enabled";
  public static final String KSQL_QUERY_PULL_RANGE_SCAN_ENABLED_DOC =
      "Config to enable range scans on table for pull queries";
  public static final boolean KSQL_QUERY_PULL_RANGE_SCAN_ENABLED_DEFAULT = true;

  public static final String KSQL_QUERY_PULL_INTERPRETER_ENABLED
      = "ksql.query.pull.interpreter.enabled";
  public static final String KSQL_QUERY_PULL_INTERPRETER_ENABLED_DOC =
      "Enables whether we use the interpreter for expression evaluation for pull queries, or the"
          + "default code generator. They should produce the same results, but the interpreter is"
          + " much faster for short-lived queries.";
  public static final boolean KSQL_QUERY_PULL_INTERPRETER_ENABLED_DEFAULT = true;

  public static final boolean KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED_DEFAULT = false;

  public static final String KSQL_QUERY_PULL_LIMIT_CLAUSE_ENABLED
          = "ksql.query.pull.limit.clause.enabled";
  public static final String KSQL_QUERY_PULL_LIMIT_CLAUSE_ENABLED_DOC
          = "Enables the use of LIMIT clause in pull queries";
  public static final boolean KSQL_QUERY_PULL_LIMIT_CLAUSE_ENABLED_DEFAULT = true;

  public static final String KSQL_QUERY_PULL_FORWARDING_TIMEOUT_MS_CONFIG
      = "ksql.query.pull.forwarding.timeout.ms";
  public static final String KSQL_QUERY_PULL_FORWARDING_TIMEOUT_MS_DOC
      = "Pull query forwarding timeout in milliseconds";
  public static final long KSQL_QUERY_PULL_FORWARDING_TIMEOUT_MS_DEFAULT =
      20000L;

  public static final String KSQL_QUERY_PUSH_V2_ENABLED
      = "ksql.query.push.v2.enabled";
  public static final String KSQL_QUERY_PUSH_V2_ENABLED_DOC =
      "Enables whether v2 push queries are enabled. The scalable form of push queries require no"
          + " window functions, aggregations, or joins, but may include projections and filters. If"
          + " they cannot be utilized, the streams application version will automatically be used"
          + " instead.";
  public static final boolean KSQL_QUERY_PUSH_V2_ENABLED_DEFAULT = false;

  public static final String KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED
      = "ksql.query.push.v2.registry.installed";
  public static final String KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED_DOC =
      "Enables whether v2 push registry should be installed. This is a requirement of "
          + "enabling scalable push queries using ksql.query.push.v2.enabled.";
  public static final boolean KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED_DEFAULT = false;

  public static final String KSQL_QUERY_PUSH_V2_ALOS_ENABLED
      = "ksql.query.push.v2.alos.enabled";
  public static final String KSQL_QUERY_PUSH_V2_ALOS_ENABLED_DOC =
      "Whether at-least-once semantics are enabled for the scalable form of push queries. "
          + "This means that a query will replay data if necessary if network or other "
          + "disruptions cause it to miss any data";
  public static final boolean KSQL_QUERY_PUSH_V2_ALOS_ENABLED_DEFAULT = true;

  public static final String KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED
      = "ksql.query.push.v2.interpreter.enabled";
  public static final String KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED_DOC =
      "Enables whether we use the interpreter for expression evaluation for scalable push queries, "
          + "or the default code generator. They should produce the same results, but may have "
          + "different performance characteristics.";
  public static final boolean KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED_DEFAULT = true;

  public static final String KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_MS
      = "ksql.query.push.v2.new.latest.delay.ms";
  public static final String KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_DOC =
      "The delay in ms for a new latest consumer to start processing records."
          + "If new nodes are added to the cluster, a longer delay makes it less likely that"
          + "stragglers requests will miss a record and be forced to catchup.";
  public static final long KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_DEFAULT = 5000;

  public static final String KSQL_QUERY_PUSH_V2_LATEST_RESET_AGE_MS
      = "ksql.query.push.v2.latest.reset.age.ms";
  public static final String KSQL_QUERY_PUSH_V2_LATEST_RESET_AGE_MS_DOC =
      "The maximum age in ms of existing committed offsets for latest consumer to"
          + " adopt those offsets rather than seek to the end.";
  public static final long KSQL_QUERY_PUSH_V2_LATEST_RESET_AGE_MS_DEFAULT = 30000;

  public static final String KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED
      = "ksql.query.push.v2.continuation.tokens.enabled";
  public static final String KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED_DOC =
      "Whether to output continuation tokens to the user.";
  public static final boolean KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED_DEFAULT = false;

  public static final String KSQL_QUERY_PUSH_V2_MAX_CATCHUP_CONSUMERS
      = "ksql.query.push.v2.max.catchup.consumers";
  public static final String KSQL_QUERY_PUSH_V2_MAX_CATCHUP_CONSUMERS_DOC =
      "The maximum number of concurrent catchup consumers.";
  public static final int KSQL_QUERY_PUSH_V2_MAX_CATCHUP_CONSUMERS_DEFAULT = 5;

  public static final String KSQL_QUERY_PUSH_V2_CATCHUP_CONSUMER_MSG_WINDOW
      = "ksql.query.push.v2.catchup.consumer.msg.window";
  public static final String KSQL_QUERY_PUSH_V2_CATCHUP_CONSUMER_MSG_WINDOW_DOC =
      "How close the catchup consumer must be to the latest before it will stop the latest to join"
          + " with it.";
  public static final int KSQL_QUERY_PUSH_V2_CATCHUP_CONSUMER_MSG_WINDOW_DEFAULT = 50;

  public static final String KSQL_QUERY_PUSH_V2_METRICS_ENABLED =
      "ksql.query.push.v2.metrics.enabled";
  public static final String KSQL_QUERY_PUSH_V2_METRICS_ENABLED_DOC =
      "Config to enable/disable collecting JMX metrics for push queries v2.";

  public static final String KSQL_STRING_CASE_CONFIG_TOGGLE = "ksql.cast.strings.preserve.nulls";
  public static final String KSQL_STRING_CASE_CONFIG_TOGGLE_DOC =
      "When casting a SQLType to string, if false, use String.valueof(), else if true use"
          + "Objects.toString()";

  public static final String KSQL_NESTED_ERROR_HANDLING_CONFIG =
      "ksql.nested.error.set.null";
  public static final String KSQL_NESTED_ERROR_HANDLING_CONFIG_DOC =
      "If there is a processing error in an element of a map, struct or array, if true set only the"
          + " failing element to null and preserve the rest of the value, else if false, set the"
          + " the entire value to null.";

  public static final String KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG =
      "ksql.streams.shutdown.timeout.ms";
  public static final Long KSQL_SHUTDOWN_TIMEOUT_MS_DEFAULT = 300_000L;
  public static final String KSQL_SHUTDOWN_TIMEOUT_MS_DOC = "Timeout in "
      + "milliseconds to block waiting for the underlying streams instance to exit";

  public static final String KSQL_AUTH_CACHE_EXPIRY_TIME_SECS =
      "ksql.authorization.cache.expiry.time.secs";
  public static final Long KSQL_AUTH_CACHE_EXPIRY_TIME_SECS_DEFAULT = 30L;
  public static final String KSQL_AUTH_CACHE_EXPIRY_TIME_SECS_DOC = "Time in "
      + "seconds to keep KSQL authorization responses in the cache. (The cache is disabled if "
      + "0 or a negative number is set).";

  public static final String KSQL_AUTH_CACHE_MAX_ENTRIES =
      "ksql.authorization.cache.max.entries";
  public static final Long KSQL_AUTH_CACHE_MAX_ENTRIES_DEFAULT = 10000L;
  public static final String KSQL_AUTH_CACHE_MAX_ENTRIES_DOC = "Controls the size of the cache "
      + "to a maximum number of KSQL authorization responses entries.";

  public static final String KSQL_HIDDEN_TOPICS_CONFIG = "ksql.hidden.topics";
  public static final String KSQL_HIDDEN_TOPICS_DEFAULT = "_confluent.*,__confluent.*"
      + ",_schemas,__consumer_offsets,__transaction_state,connect-configs,connect-offsets,"
      + "connect-status,connect-statuses";
  public static final String KSQL_HIDDEN_TOPICS_DOC = "Comma-separated list of topics that will "
      + "be hidden. Entries in the list may be literal topic names or "
      + "[Java regular expressions](https://docs.oracle.com/javase/8/docs/api/java/util/regex/"
      + "Pattern.html). "
      + "For example, `_confluent.*` will match any topic whose name starts with the `_confluent`)."
      + "\nHidden topics will not visible when running the `SHOW TOPICS` command unless "
      + "`SHOW ALL TOPICS` is used."
      + "\nThe default value hides known system topics from Kafka and Confluent products."
      + "\nKSQL also marks its own internal topics as hidden. This is not controlled by this "
      + "config.";

  public static final String KSQL_READONLY_TOPICS_CONFIG = "ksql.readonly.topics";
  public static final String KSQL_READONLY_TOPICS_DEFAULT = "_confluent.*,__confluent.*"
      + ",_schemas,__consumer_offsets,__transaction_state,connect-configs,connect-offsets,"
      + "connect-status,connect-statuses";
  public static final String KSQL_READONLY_TOPICS_DOC = "Comma-separated list of topics that "
      + "should be marked as read-only. Entries in the list may be literal topic names or "
      + "[Java regular expressions](https://docs.oracle.com/javase/8/docs/api/java/util/regex/"
      + "Pattern.html). "
      + "For example, `_confluent.*` will match any topic whose name starts with the `_confluent`)."
      + "\nRead-only topics cannot be modified by any KSQL command."
      + "\nThe default value marks known system topics from Kafka and Confluent products as "
      + "read-only."
      + "\nKSQL also marks its own internal topics as read-only. This is not controlled by this "
      + "config.";

  public static final String KSQL_TIMESTAMP_THROW_ON_INVALID = "ksql.timestamp.throw.on.invalid";
  public static final Boolean KSQL_TIMESTAMP_THROW_ON_INVALID_DEFAULT = false;
  public static final String KSQL_TIMESTAMP_THROW_ON_INVALID_DOC = "If an incoming message "
      + "contains an invalid timestamp, ksqlDB will log a warning and continue. To disable this "
      + "behavior, and instead throw an exception to ensure that no data is missed, set "
      + "ksql.timestamp.throw.on.invalid to true.";

  public static final String KSQL_ERROR_CLASSIFIER_REGEX_PREFIX = "ksql.error.classifier.regex";
  public static final String KSQL_ERROR_CLASSIFIER_REGEX_PREFIX_DOC = "Any configuration with the "
      + "regex prefix will create a new classifier that will be configured to classify anything "
      + "that matches the content as the specified type. The value must match "
      + "<TYPE><whitespace><REGEX> (for example " + KSQL_ERROR_CLASSIFIER_REGEX_PREFIX + ".invalid"
      + "=\"USER .*InvalidTopicException.*\"). The type can be one of "
      + GrammaticalJoiner.or().join(Arrays.stream(QueryError.Type.values()))
      + " and the regex pattern will be matched against the error class name and message of any "
      + "uncaught error and subsequent error causes in the Kafka Streams applications.";

  public static final String KSQL_CREATE_OR_REPLACE_ENABLED = "ksql.create.or.replace.enabled";
  public static final Boolean KSQL_CREATE_OR_REPLACE_ENABLED_DEFAULT = true;
  public static final String KSQL_CREATE_OR_REPLACE_ENABLED_DOC =
      "Feature flag for CREATE OR REPLACE";

  public static final String KSQL_METASTORE_BACKUP_LOCATION = "ksql.metastore.backup.location";
  public static final String KSQL_METASTORE_BACKUP_LOCATION_DEFAULT = "";
  public static final String KSQL_METASTORE_BACKUP_LOCATION_DOC = "Specify the directory where "
      + "KSQL metastore backup files are located.";

  public static final String KSQL_SUPPRESS_ENABLED = "ksql.suppress.enabled";
  public static final Boolean KSQL_SUPPRESS_ENABLED_DEFAULT = true;
  public static final String KSQL_SUPPRESS_ENABLED_DOC =
      "Feature flag for suppression, specifically EMIT FINAL";

  public static final String KSQL_LAMBDAS_ENABLED = "ksql.lambdas.enabled";
  public static final Boolean KSQL_LAMBDAS_ENABLED_DEFAULT = true;
  public static final String KSQL_LAMBDAS_ENABLED_DOC =
      "Feature flag for lambdas. Default is true. If true, lambdas are processed normally, "
          + "if false, new lambda queries won't be processed but any existing lambda "
          + "queries are unaffected.";

  public static final String KSQL_HEADERS_COLUMNS_ENABLED =
      "ksql.headers.columns.enabled";
  public static final Boolean KSQL_HEADERS_COLUMNS_ENABLED_DEFAULT = true;
  public static final String KSQL_HEADERS_COLUMNS_ENABLED_DOC =
      "Feature flag that allows the use of kafka headers columns on streams and tables. "
          + "If false, the HEADERS and HEADER(<key>) columns constraints won't be allowed "
          + "in CREATE statements. Current CREATE statements found in the KSQL command topic "
          + "that contains headers columns will work with the headers functionality to prevent "
          + "a degraded command topic situation when restarting ksqlDB.";

  public static final String KSQL_CLIENT_IP_PORT_CONFIGURATION_ENABLED =
      "ksql.client.ip_port.configuration.enabled";
  private static final Boolean KSQL_CLIENT_IP_PORT_CONFIGURATION_ENABLED_DEFAULT = false;
  private static final String KSQL_CLIENT_IP_PORT_CONFIGURATION_ENABLED_DOC =
      "Feature flag that enables configuration of client IP and PORT in internal ksql Kafka Client."
      + " So that Kafka broker can get client IP and PORT for logging and other purposes";

  public static final String KSQL_JSON_SR_CONVERTER_DESERIALIZER_ENABLED
      = "ksql.json_sr.converter.deserializer.enabled";

  private static final Boolean KSQL_JSON_SR_CONVERTER_DESERIALIZER_ENABLED_DEFAULT = true;

  private static final String KSQL_JSON_SR_CONVERTER_DESERIALIZER_ENABLED_DOC = ""
      + "Feature flag that enables the use of the JsonSchemaConverter class for deserializing "
      + "JSON_SR records. JsonSchemaConverter is required to support `anyOf` JSON_SR types. "
      + "This flag should be used to disable this feature only when users experience "
      + "deserialization issues caused by the JsonSchemaConverter. Otherwise, this flag should "
      + "remain true to take advantage of the new `anyOf` types and other JSON_SR serde fixes.";

  public static final String KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED =
      "ksql.source.table.materialization.enabled";
  private static final Boolean KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED_DEFAULT = true;
  private static final String KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED_DOC =
      "Feature flag that enables table materialization on source tables. Default is true. "
          + "If false, CREATE SOURCE [TABLE|STREAM] statements will be rejected. "
          + "Current CREATE SOURCE TABLE statements found in the KSQL command topic will "
          + "not be materialized and pull queries won't be allowed on them. However, current "
          + "CREATE SOURCE [TABLE|STREAM] statements will continue being read-only.";

  public static final String KSQL_SHARED_RUNTIME_ENABLED = "ksql.runtime.feature.shared.enabled";
  public static final Boolean KSQL_SHARED_RUNTIME_ENABLED_DEFAULT = false;
  public static final String KSQL_SHARED_RUNTIME_ENABLED_DOC =
      "Feature flag for sharing streams runtimes. "
          + "Default is false. If false, persistent queries will use separate "
          + " runtimes, if true, new queries may share streams instances.";

  public static final String KSQL_NEW_QUERY_PLANNER_ENABLED =
      "ksql.new.query.planner.enabled";
  private static final Boolean KSQL_NEW_QUERY_PLANNER_ENABLED_DEFAULT = false;
  private static final String KSQL_NEW_QUERY_PLANNER_ENABLED_DOC =
      "Feature flag that enables the new planner for persistent queries. Default is false.";

  public static final String KSQL_SHARED_RUNTIMES_COUNT = "ksql.shared.runtimes.count";
  public static final Integer KSQL_SHARED_RUNTIMES_COUNT_DEFAULT = 2;
  public static final String KSQL_SHARED_RUNTIMES_COUNT_DOC =
      "Controls how many runtimes queries are allocated over initially."
          + "this is only used when ksql.runtime.feature.shared.enabled is true.";


  public static final String KSQL_SUPPRESS_BUFFER_SIZE_BYTES = "ksql.suppress.buffer.size.bytes";
  public static final Long KSQL_SUPPRESS_BUFFER_SIZE_BYTES_DEFAULT = -1L;
  public static final String KSQL_SUPPRESS_BUFFER_SIZE_BYTES_DOC =
      "Bound the number of bytes that the buffer can use for suppression. Negative size means the"
      + " buffer will be unbounded. If the maximum capacity is exceeded, the query will be"
      + " terminated";

  public static final String KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS
      = "ksql.query.retry.backoff.initial.ms";
  public static final Long KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS_DEFAULT = 15000L;
  public static final String KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS_DOC = "The initial amount of time "
      + "to wait before attempting to retry a persistent query in ERROR state.";

  public static final String KSQL_QUERY_RETRY_BACKOFF_MAX_MS = "ksql.query.retry.backoff.max.ms";
  public static final Long KSQL_QUERY_RETRY_BACKOFF_MAX_MS_DEFAULT = 900000L;
  public static final String KSQL_QUERY_RETRY_BACKOFF_MAX_MS_DOC = "The maximum amount of time "
      + "to wait before attempting to retry a persistent query in ERROR state.";

  public static final String KSQL_QUERY_ERROR_MAX_QUEUE_SIZE = "ksql.query.error.max.queue.size";
  public static final Integer KSQL_QUERY_ERROR_MAX_QUEUE_SIZE_DEFAULT = 10;
  public static final String KSQL_QUERY_ERROR_MAX_QUEUE_SIZE_DOC = "The maximum number of "
      + "error messages (per query) to hold in the internal query errors queue and display"
      + "in the query description when executing the `EXPLAIN <query>` command.";

  public static final String KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS =
      "ksql.query.status.running.threshold.seconds";
  private static final Integer KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS_DEFAULT = 300;
  private static final String KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS_DOC = "Amount of time in "
      + "seconds to wait before setting a restarted query status as healthy (or running).";

  public static final String KSQL_PROPERTIES_OVERRIDES_DENYLIST =
      "ksql.properties.overrides.denylist";
  private static final String KSQL_PROPERTIES_OVERRIDES_DENYLIST_DOC = "Comma-separated list of "
      + "properties that KSQL users cannot override.";

  public static final String KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING =
      "ksql.query.persistent.max.bytes.buffering.total";
  public static final long KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_DEFAULT = -1;
  public static final String KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_DOC = "Limit on the total bytes "
      + "used by Kafka Streams cache across all persistent queries";
  public static final String KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_TRANSIENT =
      "ksql.query.transient.max.bytes.buffering.total";
  public static final long KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_TRANSIENT_DEFAULT = -1;
  public static final String KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_TRANSIENT_DOC = "Limit on the "
      + "total bytes used by Kafka Streams cache across all transient queries";

  public static final String KSQL_VARIABLE_SUBSTITUTION_ENABLE
      = "ksql.variable.substitution.enable";
  public static final boolean KSQL_VARIABLE_SUBSTITUTION_ENABLE_DEFAULT = true;
  public static final String KSQL_VARIABLE_SUBSTITUTION_ENABLE_DOC
      = "Enable variable substitution on SQL statements.";

  public static final String KSQL_QUERY_CLEANUP_SHUTDOWN_TIMEOUT_MS
      = "ksql.query.cleanup.shutdown.timeout.ms";
  public static final long KSQL_QUERY_CLEANUP_SHUTDOWN_TIMEOUT_MS_DEFAULT = 30000;
  public static final String KSQL_QUERY_CLEANUP_SHUTDOWN_TIMEOUT_MS_DOC
      = "The total time that the query cleanup spends trying to clean things up on shutdown.";

  public static final String KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_ENABLE
      = "ksql.transient.query.cleanup.service.enable";
  public static final boolean KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_ENABLE_DEFAULT = true;
  public static final String KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_ENABLE_DOC
      = "Enable transient query cleanup service.";

  public static final String KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_INITIAL_DELAY_SECONDS
      = "ksql.transient.query.cleanup.service.initial.delay.seconds";
  public static final Integer KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_INITIAL_DELAY_SECONDS_DEFAULT
      = 600;
  public static final String KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_INITIAL_DELAY_SECONDS_DOC
      = "The time to delay the first execution of the transient query cleanup service in seconds.";

  public static final String KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_PERIOD_SECONDS
      = "ksql.transient.query.cleanup.service.period.seconds";
  public static final Integer KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_PERIOD_SECONDS_DEFAULT
      = 600;
  public static final String KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_PERIOD_SECONDS_DOC
      = "the period between successive executions of the transient query cleanup service.";

  public static final String KSQL_ENDPOINT_MIGRATE_QUERY_CONFIG
      = "ksql.endpoint.migrate.query";
  private static final boolean KSQL_ENDPOINT_MIGRATE_QUERY_DEFAULT = true;
  private static final String KSQL_ENDPOINT_MIGRATE_QUERY_DOC
      = "Migrates the /query endpoint to use the same handler as /query-stream.";
  public static final String KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS
      = "ksql.assert.topic.default.timeout.ms";
  private static final int KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS_DEFAULT = 1000;
  private static final String KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS_DOC
      = "The default timeout for ASSERT TOPIC statements if no timeout is specified "
      + "in the statement.";
  public static final String KSQL_ASSERT_SCHEMA_DEFAULT_TIMEOUT_MS
      = "ksql.assert.schema.default.timeout.ms";
  private static final int KSQL_ASSERT_SCHEMA_DEFAULT_TIMEOUT_MS_DEFAULT = 1000;
  private static final String KSQL_ASSERT_SCHEMA_DEFAULT_TIMEOUT_MS_DOC
      = "The default timeout for ASSERT SCHEMA statements if no timeout is specified "
      + "in the statement.";

  public static final String KSQL_WEBSOCKET_CONNECTION_MAX_TIMEOUT_MS
      = "ksql.websocket.connection.max.timeout.ms";
  public static final long KSQL_WEBSOCKET_CONNECTION_MAX_TIMEOUT_MS_DEFAULT = 3600000;
  public static final String KSQL_WEBSOCKET_CONNECTION_MAX_TIMEOUT_MS_DOC
      = "If this config is set to a positive number, then ksqlDB will terminate websocket"
      + " connections after a timeout. The timeout will be the lower of the auth token's "
      + "lifespan (if present) and the value of this config. If this config is set to 0, then "
      + "ksqlDB will not close websockets even if the token has an expiration time.";

  public static final String KSQL_FETCH_REMOTE_HOSTS_TIMEOUT_SECONDS
      = "ksql.fetch.remote.hosts.max.timeout.seconds";
  public static final long KSQL_FETCH_REMOTE_HOSTS_TIMEOUT_SECONDS_DEFAULT = 10;
  public static final String KSQL_FETCH_REMOTE_HOSTS_TIMEOUT_SECONDS_DOC
      = "Configure how long the remote host executor will wait for in seconds "
      + "when fetching all remote hosts.";

  private enum ConfigGeneration {
    LEGACY,
    CURRENT
  }

  public static final Collection<CompatibilityBreakingConfigDef> COMPATIBLY_BREAKING_CONFIG_DEFS =
      ImmutableList.of(
          new CompatibilityBreakingConfigDef(
              KSQL_STRING_CASE_CONFIG_TOGGLE,
              Type.BOOLEAN,
              false,
              true,
              Importance.LOW,
              Optional.empty(),
              KSQL_STRING_CASE_CONFIG_TOGGLE_DOC
          ),
          new CompatibilityBreakingConfigDef(
              KSQL_NESTED_ERROR_HANDLING_CONFIG,
              Type.BOOLEAN,
              false,
              true,
              Importance.LOW,
              Optional.empty(),
              KSQL_NESTED_ERROR_HANDLING_CONFIG_DOC
          )
      );

  public static class CompatibilityBreakingConfigDef {

    private final String name;
    private final ConfigDef.Type type;
    private final Object defaultValueLegacy;
    private final Object defaultValueCurrent;
    private final ConfigDef.Importance importance;
    private final String documentation;
    private final Optional<SemanticVersion> since;
    private final Validator validator;

    CompatibilityBreakingConfigDef(
        final String name,
        final ConfigDef.Type type,
        final Object defaultValueLegacy,
        final Object defaultValueCurrent,
        final ConfigDef.Importance importance,
        final Optional<SemanticVersion> since,
        final String documentation
    ) {
      this(
          name,
          type,
          defaultValueLegacy,
          defaultValueCurrent,
          importance,
          documentation,
          since,
          null);
    }

    CompatibilityBreakingConfigDef(
        final String name,
        final ConfigDef.Type type,
        final Object defaultValueLegacy,
        final Object defaultValueCurrent,
        final ConfigDef.Importance importance,
        final String documentation,
        final Optional<SemanticVersion> since,
        final Validator validator
    ) {
      this.name = Objects.requireNonNull(name, "name");
      this.type = Objects.requireNonNull(type, "type");
      this.defaultValueLegacy = defaultValueLegacy;
      this.defaultValueCurrent = defaultValueCurrent;
      this.importance = Objects.requireNonNull(importance, "importance");
      this.documentation = Objects.requireNonNull(documentation, "documentation");
      this.since = Objects.requireNonNull(since, "since");
      this.validator = validator;
    }

    public String getName() {
      return this.name;
    }

    public Optional<SemanticVersion> since() {
      return since;
    }

    public Object getCurrentDefaultValue() {
      return defaultValueCurrent;
    }

    private void define(final ConfigDef configDef, final Object defaultValue) {
      configDef.define(name, type, defaultValue, validator, importance, documentation);
    }

    void defineLegacy(final ConfigDef configDef) {
      define(configDef, defaultValueLegacy);
    }

    void defineCurrent(final ConfigDef configDef) {
      define(configDef, defaultValueCurrent);
    }
  }

  private static final Collection<CompatibilityBreakingStreamsConfig>
      COMPATIBILITY_BREAKING_STREAMS_CONFIGS = ImmutableList.of(
          // Turn on optimizations by default, unless the user explicitly disables in config:
          new CompatibilityBreakingStreamsConfig(
              StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG,
              StreamsConfig.OPTIMIZE,
              StreamsConfig.OPTIMIZE)
  );

  private static final class CompatibilityBreakingStreamsConfig {
    final String name;
    final Object defaultValueLegacy;
    final Object defaultValueCurrent;

    CompatibilityBreakingStreamsConfig(final String name, final Object defaultValueLegacy,
        final Object defaultValueCurrent) {
      this.name = Objects.requireNonNull(name);
      if (!StreamsConfig.configDef().names().contains(name)) {
        throw new IllegalArgumentException(
            String.format("%s is not a valid streams config", name));
      }
      this.defaultValueLegacy = defaultValueLegacy;
      this.defaultValueCurrent = defaultValueCurrent;
    }

    String getName() {
      return this.name;
    }
  }

  public static final ConfigDef CURRENT_DEF = buildConfigDef(ConfigGeneration.CURRENT);
  public static final ConfigDef LEGACY_DEF = buildConfigDef(ConfigGeneration.LEGACY);
  public static final Set<String> SSL_CONFIG_NAMES = sslConfigNames();
  public static final Set<String> STREAM_TOPIC_CONFIG_NAMES = streamTopicConfigNames();

  public static KsqlConfig empty() {
    return new KsqlConfig(ImmutableMap.of());
  }

  private static ConfigDef configDef(final ConfigGeneration generation) {
    return generation == ConfigGeneration.CURRENT ? CURRENT_DEF : LEGACY_DEF;
  }

  private static DeploymentType parseDeploymentType(final Object value) {
    try {
      return DeploymentType.valueOf(value.toString());
    } catch (IllegalArgumentException e) {
      throw new ConfigException(
          "'" + KSQL_DEPLOYMENT_TYPE_CONFIG + "' must be one of: "
          + Arrays.asList(DeploymentType.values())
          + " however we found '" + value + "'"
      );
    }
  }

  // CHECKSTYLE_RULES.OFF: MethodLength
  private static ConfigDef buildConfigDef(final ConfigGeneration generation) {
    final ConfigDef configDef = new ConfigDef()
        .define(
            KSQL_DEPLOYMENT_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            DeploymentType.selfManaged.name(),
            ConfigDef.LambdaValidator.with(
                (name, value) -> parseDeploymentType(value),
                () -> Arrays.asList(DeploymentType.values()).toString()
            ),
            ConfigDef.Importance.LOW,
            KSQL_DEPLOYMENT_TYPE_DOC
        ).define(
            KSQL_SERVICE_ID_CONFIG,
            ConfigDef.Type.STRING,
            KSQL_SERVICE_ID_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Indicates the ID of the ksql service. It will be used as prefix for "
                + "all implicitly named resources created by this instance in Kafka. "
                + "By convention, the id should end in a seperator character of some form, e.g. "
                + "a dash or underscore, as this makes identifiers easier to read."
        ).define(
            KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            KSQL_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Second part of the prefix for transient queries. For instance if "
            + "the prefix is transient_ the query name would be "
            + "ksql_transient_4120896722607083946_1509389010601 where 'ksql_' is the first prefix"
            + " and '_transient' is the second part of the prefix for the query id the third and "
            + "4th parts are a random long value and the current timestamp. "
        ).define(
            KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.LOW,
            KSQL_OUTPUT_TOPIC_NAME_PREFIX_DOCS
        ).define(
            SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY,
            ConfigDef.Type.LONG,
            KsqlConstants.defaultSinkWindowChangeLogAdditionalRetention,
            ConfigDef.Importance.MEDIUM,
            "The default window change log additional retention time. This "
            + "is a streams config value which will be added to a windows maintainMs to ensure "
            + "data is not deleted from the log prematurely. Allows for clock drift. "
            + "Default is 1 day"
        ).define(
            SCHEMA_REGISTRY_URL_PROPERTY,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.MEDIUM,
            "The URL for the schema registry"
        ).define(
            CONNECT_URL_PROPERTY,
            ConfigDef.Type.STRING,
            DEFAULT_CONNECT_URL,
            Importance.MEDIUM,
            "The URL for the connect deployment, defaults to http://localhost:8083"
        ).define(
            CONNECT_WORKER_CONFIG_FILE_PROPERTY,
            ConfigDef.Type.STRING,
            "",
            Importance.LOW,
            "The path to a connect worker configuration file. An empty value for this configuration"
                + "will prevent connect from starting up embedded within KSQL. For more information"
                + " on configuring connect, see "
                + "https://docs.confluent.io/current/connect/userguide.html#configuring-workers."
        ).define(
            CONNECT_BASIC_AUTH_CREDENTIALS_SOURCE_PROPERTY,
            ConfigDef.Type.STRING,
            BASIC_AUTH_CREDENTIALS_SOURCE_NONE,
            BASIC_AUTH_CREDENTIALS_SOURCE_VALIDATOR,
            Importance.LOW,
            "If providing explicit basic auth credentials for ksqlDB to use when sending connector "
                + "requests, this config specifies how credentials should be loaded. Valid "
                + "options are 'FILE' in order to specify the username and password in a "
                + "properties file, or 'NONE' to indicate that custom basic auth should "
                + "not be used. If 'NONE', ksqlDB will forward the auth header, if present, "
                + "on the incoming ksql request to Connect."
        ).define(
            CONNECT_BASIC_AUTH_CREDENTIALS_FILE_PROPERTY,
            ConfigDef.Type.STRING,
            "",
            Importance.LOW,
            "If '" + CONNECT_BASIC_AUTH_CREDENTIALS_SOURCE_PROPERTY + "' is set to 'FILE', "
                + "then this config specifies the path to the credentials file."
        ).define(
            CONNECT_BASIC_AUTH_CREDENTIALS_RELOAD_PROPERTY,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.LOW,
            "If true, basic auth credentials for connector auth will automatically reload "
                + "on file change (creation or modification). File deletion is not monitored and "
                + "old credentials will continue to be used in this case."
        ).define(
            CONNECT_REQUEST_TIMEOUT_MS,
            Type.LONG,
            CONNECT_REQUEST_TIMEOUT_DEFAULT,
            ConfigDef.Importance.LOW,
            CONNECT_REQUEST_TIMEOUT_MS_DOC
        ).define(
            CONNECT_REQUEST_HEADERS_PLUGIN,
            Type.CLASS,
            null,
            ConfigDef.Importance.LOW,
            CONNECT_REQUEST_HEADERS_PLUGIN_DOC
        ).define(
            KSQL_ENABLE_UDFS,
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.MEDIUM,
            "Whether or not custom UDF jars found in the ext dir should be loaded. Default is true "
        ).define(
            KSQL_COLLECT_UDF_METRICS,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.LOW,
            "Whether or not metrics should be collected for custom udfs. Default is false. Note: "
                + "this will add some overhead to udf invocation. It is recommended that this "
                + " be set to false in production."
        ).define(
            KSQL_EXT_DIR,
            ConfigDef.Type.STRING,
            DEFAULT_EXT_DIR,
            ConfigDef.Importance.LOW,
            "The path to look for and load extensions such as UDFs from."
        ).define(
            KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY,
            Type.SHORT,
            (short) 1,
            ConfigDef.Importance.MEDIUM,
            "The replication factor for the internal topics of KSQL server."
        ).define(
            KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS_PROPERTY,
            Type.SHORT,
            (short) 1,
            ConfigDef.Importance.MEDIUM,
            "The minimum number of insync replicas for the internal topics of KSQL server."
        ).define(
            KSQL_UDF_SECURITY_MANAGER_ENABLED,
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.LOW,
            "Enable the security manager for UDFs. Default is true and will stop UDFs from"
               + " calling System.exit or executing processes"
        ).define(
            KSQL_INSERT_INTO_VALUES_ENABLED,
            Type.BOOLEAN,
            true,
            ConfigDef.Importance.LOW,
            "Enable the INSERT INTO ... VALUES functionality."
        ).define(
            KSQL_SECURITY_EXTENSION_CLASS,
            Type.CLASS,
            KSQL_SECURITY_EXTENSION_DEFAULT,
            ConfigDef.Importance.LOW,
            KSQL_SECURITY_EXTENSION_DOC
        ).define(
            KSQL_DEFAULT_KEY_FORMAT_CONFIG,
            Type.STRING,
            KSQL_DEFAULT_KEY_FORMAT_DEFAULT,
            ConfigDef.Importance.LOW,
            KSQL_DEFAULT_KEY_FORMAT_DOC
        ).define(
            KSQL_DEFAULT_VALUE_FORMAT_CONFIG,
            Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            KSQL_DEFAULT_VALUE_FORMAT_DOC
        ).define(
            KSQL_WRAP_SINGLE_VALUES,
            ConfigDef.Type.BOOLEAN,
            null,
            ConfigDef.Importance.LOW,
            "Controls how KSQL will serialize a value whose schema contains only a "
                + "single column. The setting only sets the default for `CREATE STREAM`, "
                + "`CREATE TABLE`, `CREATE STREAM AS SELECT`, `CREATE TABLE AS SELECT` and "
                + "`INSERT INTO` statements, where `WRAP_SINGLE_VALUE` is not provided explicitly "
                + "in the statement." + System.lineSeparator()
                + "When set to true, KSQL will persist the single column nested with a STRUCT, "
                + "for formats that support them. When set to false KSQL will persist "
                + "the column as the anonymous values." + System.lineSeparator()
                + "For example, if the value contains only a single column 'FOO INT' and the "
                + "format is JSON,  and this setting is `false`, then KSQL will persist the value "
                + "as an unnamed JSON number, e.g. '10'. Where as, if this setting is `true`, KSQL "
                + "will persist the value as a JSON document with a single numeric property, "
                + "e.g. '{\"FOO\": 10}." + System.lineSeparator()
                + "Note: the DELIMITED format ignores this setting as it does not support the "
                + "concept of a STRUCT, record or object."
        ).define(
            KSQL_CUSTOM_METRICS_TAGS,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.LOW,
            KSQL_CUSTOM_METRICS_TAGS_DOC
        ).define(
            KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE_DOC
        ).define(
            KSQL_QUERYANONYMIZER_ENABLED,
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.LOW,
            KSQL_QUERYANONYMIZER_ENABLED_DOC
        ).define(
            KSQL_CUSTOM_METRICS_EXTENSION,
            ConfigDef.Type.CLASS,
            null,
            ConfigDef.Importance.LOW,
            KSQL_CUSTOM_METRICS_EXTENSION_DOC
        ).define(
            KSQL_ENABLE_ACCESS_VALIDATOR,
            Type.STRING,
            KSQL_ACCESS_VALIDATOR_AUTO,
            ValidString.in(
                KSQL_ACCESS_VALIDATOR_ON,
                KSQL_ACCESS_VALIDATOR_OFF,
                KSQL_ACCESS_VALIDATOR_AUTO
            ),
            ConfigDef.Importance.LOW,
            KSQL_ACCESS_VALIDATOR_DOC
        ).define(METRIC_REPORTER_CLASSES_CONFIG,
            Type.LIST,
            "",
            Importance.LOW,
            METRIC_REPORTER_CLASSES_DOC
        ).define(
            KSQL_PULL_QUERIES_ENABLE_CONFIG,
            Type.BOOLEAN,
            KSQL_QUERY_PULL_ENABLE_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PULL_ENABLE_DOC
        ).define(
            KSQL_QUERY_PULL_ENABLE_STANDBY_READS,
            Type.BOOLEAN,
            KSQL_QUERY_PULL_ENABLE_STANDBY_READS_DEFAULT,
            Importance.MEDIUM,
            KSQL_QUERY_PULL_ENABLE_STANDBY_READS_DOC
        ).define(
            KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG,
            Type.LONG,
            KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_DEFAULT,
            zeroOrPositive(),
            Importance.MEDIUM,
            KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_DOC
        ).define(
            KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG,
            Type.STRING,
            KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT,
            Importance.LOW,
            KSQL_PERSISTENT_QUERY_NAME_PREFIX_DOC
        ).define(
            KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG,
            Type.INT,
            KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DEFAULT,
            Importance.MEDIUM,
            KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DOC
        ).define(
            KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG,
            Type.LONG,
            KSQL_SHUTDOWN_TIMEOUT_MS_DEFAULT,
            Importance.MEDIUM,
            KSQL_SHUTDOWN_TIMEOUT_MS_DOC
        ).define(
            KSQL_AUTH_CACHE_EXPIRY_TIME_SECS,
            Type.LONG,
            KSQL_AUTH_CACHE_EXPIRY_TIME_SECS_DEFAULT,
            Importance.LOW,
            KSQL_AUTH_CACHE_EXPIRY_TIME_SECS_DOC
        ).define(
            KSQL_AUTH_CACHE_MAX_ENTRIES,
            Type.LONG,
            KSQL_AUTH_CACHE_MAX_ENTRIES_DEFAULT,
            Importance.LOW,
            KSQL_AUTH_CACHE_MAX_ENTRIES_DOC
        ).define(
            KSQL_HIDDEN_TOPICS_CONFIG,
            Type.LIST,
            KSQL_HIDDEN_TOPICS_DEFAULT,
            ConfigValidators.validRegex(),
            Importance.LOW,
            KSQL_HIDDEN_TOPICS_DOC
        ).define(
            KSQL_READONLY_TOPICS_CONFIG,
            Type.LIST,
            KSQL_READONLY_TOPICS_DEFAULT,
            ConfigValidators.validRegex(),
            Importance.LOW,
            KSQL_READONLY_TOPICS_DOC
        )
        .define(
            KSQL_TIMESTAMP_THROW_ON_INVALID,
            Type.BOOLEAN,
            KSQL_TIMESTAMP_THROW_ON_INVALID_DEFAULT,
            Importance.MEDIUM,
            KSQL_TIMESTAMP_THROW_ON_INVALID_DOC
        )
        .define(
            KSQL_QUERY_PULL_METRICS_ENABLED,
            Type.BOOLEAN,
            true,
            Importance.LOW,
            KSQL_QUERY_PULL_METRICS_ENABLED_DOC
        )
        .define(
            KSQL_QUERY_PULL_MAX_QPS_CONFIG,
            Type.INT,
            KSQL_QUERY_PULL_MAX_QPS_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PULL_MAX_QPS_DOC
        )
        .define(
            KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_CONFIG,
            Type.INT,
            KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_DOC
        )
        .define(
            KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG,
            Type.INT,
            KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_DEFAULT,
            Importance.HIGH,
            KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_DOC
        )
        .define(
            KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG,
        Type.INT,
            KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_DEFAULT,
        Importance.HIGH,
            KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_DOC
        )
        .define(
            KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG,
            Type.INT,
            KSQL_QUERY_PULL_THREAD_POOL_SIZE_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PULL_THREAD_POOL_SIZE_DOC
        )
        .define(
            KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_CONFIG,
            Type.INT,
            KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_DOC
        )
        .define(
            KSQL_QUERY_PULL_TABLE_SCAN_ENABLED,
            Type.BOOLEAN,
            KSQL_QUERY_PULL_TABLE_SCAN_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PULL_TABLE_SCAN_ENABLED_DOC
        )
        .define(
            KSQL_QUERY_STREAM_PULL_QUERY_ENABLED,
            Type.BOOLEAN,
            KSQL_QUERY_STREAM_PULL_QUERY_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_STREAM_PULL_QUERY_ENABLED_DOC
        )
        .define(
            KSQL_QUERY_PULL_RANGE_SCAN_ENABLED,
            Type.BOOLEAN,
            KSQL_QUERY_PULL_RANGE_SCAN_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PULL_RANGE_SCAN_ENABLED_DOC
        )
        .define(
            KSQL_QUERY_PULL_INTERPRETER_ENABLED,
            Type.BOOLEAN,
            KSQL_QUERY_PULL_INTERPRETER_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PULL_INTERPRETER_ENABLED_DOC
        )
        .define(
            KSQL_QUERY_PULL_LIMIT_CLAUSE_ENABLED,
            Type.BOOLEAN,
            KSQL_QUERY_PULL_LIMIT_CLAUSE_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PULL_LIMIT_CLAUSE_ENABLED_DOC
        )
        .define(
            KSQL_QUERY_PULL_FORWARDING_TIMEOUT_MS_CONFIG,
            Type.LONG,
            KSQL_QUERY_PULL_FORWARDING_TIMEOUT_MS_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PULL_FORWARDING_TIMEOUT_MS_DOC
        )
        .define(
            KSQL_QUERY_PUSH_V2_ENABLED,
            Type.BOOLEAN,
            KSQL_QUERY_PUSH_V2_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PUSH_V2_ENABLED_DOC
        )
        .define(
            KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED,
            Type.BOOLEAN,
            KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED_DOC
        )
        .define(
            KSQL_QUERY_PUSH_V2_ALOS_ENABLED,
            Type.BOOLEAN,
            KSQL_QUERY_PUSH_V2_ALOS_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PUSH_V2_ALOS_ENABLED_DOC
        )
        .define(
            KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED,
            Type.BOOLEAN,
            KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED_DOC
        )
        .define(
            KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_MS,
            Type.LONG,
            KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_DOC
        )
        .define(
            KSQL_QUERY_PUSH_V2_LATEST_RESET_AGE_MS,
            Type.LONG,
            KSQL_QUERY_PUSH_V2_LATEST_RESET_AGE_MS_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PUSH_V2_LATEST_RESET_AGE_MS_DOC
        )
        .define(
            KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED,
            Type.BOOLEAN,
            KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED_DOC
        )
        .define(
            KSQL_QUERY_PUSH_V2_MAX_CATCHUP_CONSUMERS,
            Type.INT,
            KSQL_QUERY_PUSH_V2_MAX_CATCHUP_CONSUMERS_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PUSH_V2_MAX_CATCHUP_CONSUMERS_DOC
        )
        .define(
            KSQL_QUERY_PUSH_V2_CATCHUP_CONSUMER_MSG_WINDOW,
            Type.LONG,
            KSQL_QUERY_PUSH_V2_CATCHUP_CONSUMER_MSG_WINDOW_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PUSH_V2_CATCHUP_CONSUMER_MSG_WINDOW_DOC
        )
        .define(
            KSQL_QUERY_PUSH_V2_METRICS_ENABLED,
            Type.BOOLEAN,
            true,
            Importance.LOW,
            KSQL_QUERY_PUSH_V2_METRICS_ENABLED_DOC
        )
        .define(
            KSQL_ERROR_CLASSIFIER_REGEX_PREFIX,
            Type.STRING,
            "",
            Importance.LOW,
            KSQL_ERROR_CLASSIFIER_REGEX_PREFIX_DOC
        )
        .define(
            KSQL_CREATE_OR_REPLACE_ENABLED,
            Type.BOOLEAN,
            KSQL_CREATE_OR_REPLACE_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_CREATE_OR_REPLACE_ENABLED_DOC
        )
        .define(
            KSQL_METASTORE_BACKUP_LOCATION,
            Type.STRING,
            KSQL_METASTORE_BACKUP_LOCATION_DEFAULT,
            Importance.LOW,
            KSQL_METASTORE_BACKUP_LOCATION_DOC
        )
        .define(
            KSQL_SUPPRESS_ENABLED,
            Type.BOOLEAN,
            KSQL_SUPPRESS_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_SUPPRESS_ENABLED_DOC
        )
        .define(
            KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS,
            Type.LONG,
            KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS_DOC
        )
        .define(
            KSQL_QUERY_RETRY_BACKOFF_MAX_MS,
            Type.LONG,
            KSQL_QUERY_RETRY_BACKOFF_MAX_MS_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_RETRY_BACKOFF_MAX_MS_DOC
        )
        .define(
            KSQL_QUERY_ERROR_MAX_QUEUE_SIZE,
            Type.INT,
            KSQL_QUERY_ERROR_MAX_QUEUE_SIZE_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_ERROR_MAX_QUEUE_SIZE_DOC
        )
        .define(
            KSQL_SUPPRESS_BUFFER_SIZE_BYTES,
            Type.LONG,
            KSQL_SUPPRESS_BUFFER_SIZE_BYTES_DEFAULT,
            Importance.LOW,
            KSQL_SUPPRESS_BUFFER_SIZE_BYTES_DOC
        )
        .define(
            KSQL_PROPERTIES_OVERRIDES_DENYLIST,
            Type.LIST,
            "",
            Importance.LOW,
            KSQL_PROPERTIES_OVERRIDES_DENYLIST_DOC
        )
        .define(
            KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS,
            Type.INT,
            KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS_DOC
        )
        .define(
            KSQL_VARIABLE_SUBSTITUTION_ENABLE,
            Type.BOOLEAN,
            KSQL_VARIABLE_SUBSTITUTION_ENABLE_DEFAULT,
            Importance.LOW,
            KSQL_VARIABLE_SUBSTITUTION_ENABLE_DOC
        )
        .define(
            KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING,
            Type.LONG,
            KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_DEFAULT,
            Importance.LOW,
            KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_DOC
        )
        .define(
            KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_TRANSIENT,
            Type.LONG,
            KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_TRANSIENT_DEFAULT,
            Importance.LOW,
            KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_TRANSIENT_DOC
        ).define(
            KSQL_LAMBDAS_ENABLED,
            Type.BOOLEAN,
            KSQL_LAMBDAS_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_LAMBDAS_ENABLED_DOC)
        .define(
            KSQL_SHARED_RUNTIME_ENABLED,
            Type.BOOLEAN,
            KSQL_SHARED_RUNTIME_ENABLED_DEFAULT,
            Importance.MEDIUM,
            KSQL_SHARED_RUNTIME_ENABLED_DOC
        )
        .define(
            KSQL_SHARED_RUNTIMES_COUNT,
            Type.INT,
            KSQL_SHARED_RUNTIMES_COUNT_DEFAULT,
            Importance.MEDIUM,
            KSQL_SHARED_RUNTIMES_COUNT_DOC
        )
        .define(
            KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED,
            Type.BOOLEAN,
            KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED_DOC
        )
        .define(
            KSQL_NEW_QUERY_PLANNER_ENABLED,
            Type.BOOLEAN,
            KSQL_NEW_QUERY_PLANNER_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_NEW_QUERY_PLANNER_ENABLED_DOC
        )
        .define(
            KSQL_QUERY_CLEANUP_SHUTDOWN_TIMEOUT_MS,
            Type.LONG,
            KSQL_QUERY_CLEANUP_SHUTDOWN_TIMEOUT_MS_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_CLEANUP_SHUTDOWN_TIMEOUT_MS_DOC
        )
        .define(
            KSQL_HEADERS_COLUMNS_ENABLED,
            Type.BOOLEAN,
            KSQL_HEADERS_COLUMNS_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_HEADERS_COLUMNS_ENABLED_DOC
        )
        .define(
            KSQL_ENDPOINT_MIGRATE_QUERY_CONFIG,
            Type.BOOLEAN,
            KSQL_ENDPOINT_MIGRATE_QUERY_DEFAULT,
            Importance.LOW,
            KSQL_ENDPOINT_MIGRATE_QUERY_DOC
        )
        .define(
            KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_ENABLE,
            Type.BOOLEAN,
            KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_ENABLE_DEFAULT,
            Importance.LOW,
            KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_ENABLE_DOC
        )
        .define(
            KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_INITIAL_DELAY_SECONDS,
            Type.INT,
            KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_INITIAL_DELAY_SECONDS_DEFAULT,
            Importance.LOW,
            KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_INITIAL_DELAY_SECONDS_DOC
        )
        .define(
            KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_PERIOD_SECONDS,
            Type.INT,
            KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_PERIOD_SECONDS_DEFAULT,
            Importance.LOW,
            KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_PERIOD_SECONDS_DOC
        )
        .define(
            KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS,
            Type.INT,
            KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS_DEFAULT,
            Importance.LOW,
            KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS_DOC
        )
        .define(
            KSQL_ASSERT_SCHEMA_DEFAULT_TIMEOUT_MS,
            Type.INT,
            KSQL_ASSERT_SCHEMA_DEFAULT_TIMEOUT_MS_DEFAULT,
            Importance.LOW,
            KSQL_ASSERT_SCHEMA_DEFAULT_TIMEOUT_MS_DOC
        )
        .define(
            KSQL_WEBSOCKET_CONNECTION_MAX_TIMEOUT_MS,
            Type.LONG,
            KSQL_WEBSOCKET_CONNECTION_MAX_TIMEOUT_MS_DEFAULT,
            Importance.LOW,
            KSQL_WEBSOCKET_CONNECTION_MAX_TIMEOUT_MS_DOC
        )
        .define(
            KSQL_JSON_SR_CONVERTER_DESERIALIZER_ENABLED,
            Type.BOOLEAN,
            KSQL_JSON_SR_CONVERTER_DESERIALIZER_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_JSON_SR_CONVERTER_DESERIALIZER_ENABLED_DOC
        )
        .define(
            KSQL_CLIENT_IP_PORT_CONFIGURATION_ENABLED,
            Type.BOOLEAN,
            KSQL_CLIENT_IP_PORT_CONFIGURATION_ENABLED_DEFAULT,
            Importance.LOW,
            KSQL_CLIENT_IP_PORT_CONFIGURATION_ENABLED_DOC
        )
        .define(
            KSQL_FETCH_REMOTE_HOSTS_TIMEOUT_SECONDS,
            Type.LONG,
            KSQL_FETCH_REMOTE_HOSTS_TIMEOUT_SECONDS_DEFAULT,
            Importance.LOW,
            KSQL_FETCH_REMOTE_HOSTS_TIMEOUT_SECONDS_DOC
        )
        .withClientSslSupport()
        .define(
            ConfluentConfigs.ENABLE_FIPS_CONFIG,
            Type.BOOLEAN,
            ConfluentConfigs.ENABLE_FIPS_DEFAULT,
            Importance.LOW,
            ConfluentConfigs.ENABLE_FIPS_DOC
        );

    for (final CompatibilityBreakingConfigDef compatibilityBreakingConfigDef
        : COMPATIBLY_BREAKING_CONFIG_DEFS) {
      if (generation == ConfigGeneration.CURRENT) {
        compatibilityBreakingConfigDef.defineCurrent(configDef);
      } else {
        compatibilityBreakingConfigDef.defineLegacy(configDef);
      }
    }
    return configDef;
  }
  // CHECKSTYLE_RULES.ON: MethodLength

  public Map<String, Object> originalsWithPrefixOverride(final String prefix) {
    final Map<String, Object> originals = originals();
    final Map<String, Object> result = new HashMap<>();
    // first we iterate over the originals and we add only the entries without the prefix
    for (Map.Entry<String, ?> entry : originals.entrySet()) {
      if (!isKeyPrefixed(entry.getKey(), prefix)) {
        result.put(entry.getKey(), entry.getValue());
      }
    }
    // then we add only prefixed entries with dropped prefix
    for (Map.Entry<String, ?> entry : originals.entrySet()) {
      if (isKeyPrefixed(entry.getKey(), prefix)) {
        result.put(entry.getKey().substring(prefix.length()), entry.getValue());
      }
    }
    // two iterations are necessary to avoid a situation where the unprefixed value
    // is handled after the prefixed one, because we do not control the order in which
    // the entries are presented from the originals map
    return result;
  }

  private boolean isKeyPrefixed(final String key, final String prefix) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(prefix);
    return key.startsWith(prefix) && key.length() > prefix.length();
  }

  private static final class ConfigValue {
    final ConfigItem configItem;
    final String key;
    final Object value;

    private ConfigValue(final ConfigItem configItem, final String key, final Object value) {
      this.configItem = configItem;
      this.key = key;
      this.value = value;
    }

    private boolean isResolved() {
      return configItem.isResolved();
    }

    private String convertToObfuscatedString() {
      return configItem.convertToString(value);
    }
  }

  private static void applyStreamsConfig(
      final Map<String, ?> props,
      final Map<String, ConfigValue> streamsConfigProps) {
    props.entrySet()
        .stream()
        .map(e -> resolveStreamsConfig(e.getKey(), e.getValue()))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(
            configValue -> streamsConfigProps.put(configValue.key, configValue));
  }

  private static Optional<ConfigValue> resolveStreamsConfig(
      final String maybePrefixedKey,
      final Object value) {
    final String key = maybePrefixedKey.startsWith(KSQL_STREAMS_PREFIX)
        ? maybePrefixedKey.substring(KSQL_STREAMS_PREFIX.length()) : maybePrefixedKey;

    if (key.startsWith(KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX)) {
      return Optional.empty();
    }

    return new KsqlConfigResolver().resolve(maybePrefixedKey, false)
        .map(configItem -> new ConfigValue(configItem, key, configItem.parseValue(value)));
  }

  private static Map<String, ConfigValue> buildStreamingConfig(
      final Map<String, ?> baseStreamConfig,
      final Map<String, ?> overrides) {
    final Map<String, ConfigValue> streamConfigProps = new HashMap<>();
    applyStreamsConfig(baseStreamConfig, streamConfigProps);
    applyStreamsConfig(overrides, streamConfigProps);
    return ImmutableMap.copyOf(streamConfigProps);
  }

  private final Map<String, ConfigValue> ksqlStreamConfigProps;

  public KsqlConfig(final Map<?, ?> props) {
    this(ConfigGeneration.CURRENT, props);
  }

  private KsqlConfig(final ConfigGeneration generation, final Map<?, ?> props) {
    super(configDef(generation), props);

    final Map<String, Object> streamsConfigDefaults = new HashMap<>();
    streamsConfigDefaults.put(StreamsConfig.InternalConfig.STATE_UPDATER_ENABLED, false);
    streamsConfigDefaults.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, KsqlConstants
        .defaultCommitIntervalMsConfig);
    streamsConfigDefaults.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, KsqlConstants
            .defaultCacheMaxBytesBufferingConfig);
    streamsConfigDefaults.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, KsqlConstants
        .defaultNumberOfStreamsThreads);
    if (!getBooleanConfig(FAIL_ON_DESERIALIZATION_ERROR_CONFIG, false)) {
      streamsConfigDefaults.put(
          StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
          LogMetricAndContinueExceptionHandler.class
      );
    }
    streamsConfigDefaults.put(
        StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
        ProductionExceptionHandlerUtil.getHandler(
            getBooleanConfig(FAIL_ON_PRODUCTION_ERROR_CONFIG, true))
    );
    COMPATIBILITY_BREAKING_STREAMS_CONFIGS.forEach(
        config -> streamsConfigDefaults.put(
            config.name,
            generation == ConfigGeneration.CURRENT
                ? config.defaultValueCurrent : config.defaultValueLegacy));
    this.ksqlStreamConfigProps = buildStreamingConfig(streamsConfigDefaults,
            originalsWithPrefixOverride(KSQL_STREAMS_PREFIX));
  }

  private static Set<String> streamTopicConfigNames() {
    final ImmutableSet.Builder<String> configs = ImmutableSet.builder();

    Arrays.stream(TopicConfig.class.getDeclaredFields())
        .filter(f -> f.getType() == String.class)
        .filter(f -> f.getName().endsWith("_CONFIG"))
        .forEach(f -> {
          try {
            configs.add((String) f.get(null));
          } catch (final IllegalAccessException e) {
            LOG.warn("Could not get the '{}' config from TopicConfig.class. Setting and listing "
                + "properties with 'ksql.streams.topics.*' from clients will be disabled.",
                f.getName());
          }
        });

    return configs.build();
  }

  private boolean getBooleanConfig(final String config, final boolean defaultValue) {
    final Object value = originals().get(config);
    if (value == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value.toString());
  }

  private KsqlConfig(final ConfigGeneration generation,
                     final Map<String, ?> values,
                     final Map<String, ConfigValue> ksqlStreamConfigProps) {
    super(configDef(generation), values);
    this.ksqlStreamConfigProps = ksqlStreamConfigProps;
  }

  private void possiblyConfigureConfluentTelemetry(final Map<String, Object> map) {
    if (KsqlConfig.DeploymentType.confluent.toString()
            .equals(getString(KSQL_DEPLOYMENT_TYPE_CONFIG))) {
      final List<String> metricReporters = new ArrayList<>(getList(METRIC_REPORTER_CLASSES_CONFIG));
      metricReporters.remove(TELEMETRY_REPORTER_CLASS);
      map.put(METRIC_REPORTER_CLASSES_CONFIG, metricReporters);
    } else {
      map.putAll(addConfluentMetricsContextConfigsKafka(Collections.emptyMap()));
    }
  }

  public Map<String, Object> getKsqlStreamConfigProps(final String applicationId) {
    final Map<String, Object> map = new HashMap<>(getKsqlStreamConfigProps());
    map.put(
        MetricCollectors.RESOURCE_LABEL_PREFIX
            + StreamsConfig.APPLICATION_ID_CONFIG,
        applicationId
    );

    // Streams client metrics aren't used in Confluent deployment
    possiblyConfigureConfluentTelemetry(map);
    return Collections.unmodifiableMap(map);
  }

  public Map<String, Object> getKsqlStreamConfigProps() {
    final Map<String, Object> map = new HashMap<>();
    for (final ConfigValue config : ksqlStreamConfigProps.values()) {
      map.put(config.key, config.value);
    }
    return Collections.unmodifiableMap(map);
  }

  public Optional<Object> getKsqlStreamConfigProp(final String key) {
    return Optional.ofNullable(ksqlStreamConfigProps.get(key)).map(cv -> cv.value);
  }

  public Map<String, Object> getKsqlAdminClientConfigProps() {
    final Map<String, Object> map = new HashMap<>();
    map.putAll(getConfigsFor(AdminClientConfig.configNames()));
    // admin client metrics aren't used in Confluent deployment
    possiblyConfigureConfluentTelemetry(map);
    return Collections.unmodifiableMap(map);
  }

  public Map<String, Object> getProducerClientConfigProps() {
    final Map<String, Object> map = new HashMap<>();
    map.putAll(getConfigsFor(ProducerConfig.configNames()));
    // producer client metrics aren't used in Confluent deployment
    possiblyConfigureConfluentTelemetry(map);
    return Collections.unmodifiableMap(map);
  }

  public Map<String, Object> getConsumerClientConfigProps() {
    final Map<String, Object> map = new HashMap<>();
    map.putAll(getConfigsFor(ConsumerConfig.configNames()));
    // consumer client metrics aren't used in Confluent deployment
    possiblyConfigureConfluentTelemetry(map);
    return Collections.unmodifiableMap(map);
  }

  public Map<String, Object> addConfluentMetricsContextConfigsKafka(
      final Map<String,Object> props
  ) {
    final Map<String, Object> updatedProps = new HashMap<>(props);
    final AppInfoParser.AppInfo appInfo = new AppInfoParser.AppInfo(System.currentTimeMillis());
    updatedProps.putAll(getConfigsForPrefix(REPORTER_CONFIGS_PREFIXES));
    updatedProps.put(MetricCollectors.RESOURCE_LABEL_VERSION, appInfo.getVersion());
    updatedProps.put(MetricCollectors.RESOURCE_LABEL_COMMIT_ID, appInfo.getCommitId());
    return updatedProps;
  }

  public Map<String, Object> getProcessingLogConfigProps() {
    return getConfigsFor(ProcessingLogConfig.configNames());
  }

  private Map<String, Object> getConfigsFor(final Set<String> configs) {
    final Map<String, Object> props = new HashMap<>();
    ksqlStreamConfigProps.values().stream()
        .filter(configValue -> configs.contains(configValue.key))
        .forEach(configValue -> props.put(configValue.key, configValue.value));
    return Collections.unmodifiableMap(props);
  }

  private Map<String, Object> getConfigsForPrefix(final Set<String> configs) {
    final Map<String, Object> props = new HashMap<>();
    ksqlStreamConfigProps.values().stream()
        .filter(configValue -> configs.stream().anyMatch(configValue.key::startsWith))
        .forEach(configValue -> props.put(configValue.key, configValue.value));
    return Collections.unmodifiableMap(props);
  }

  public Map<String, Object> getKsqlFunctionsConfigProps(final String functionName) {
    final Map<String, Object> udfProps = originalsWithPrefix(
        KSQL_FUNCTIONS_PROPERTY_PREFIX + functionName.toLowerCase(), false);

    final Map<String, Object> globals = originalsWithPrefix(
        KSQ_FUNCTIONS_GLOBAL_PROPERTY_PREFIX, false);

    udfProps.putAll(globals);

    return udfProps;
  }

  private Map<String, String> getKsqlConfigPropsWithSecretsObfuscated() {
    final Map<String, String> props = new HashMap<>();

    originalsWithPrefix(KSQL_FUNCTIONS_PROPERTY_PREFIX, false)
        .forEach((key, value) -> props.put(key, "[hidden]"));

    configDef(ConfigGeneration.CURRENT).names().stream()
        .filter(key -> !SSL_CONFIG_NAMES.contains(key))
        .forEach(
            key -> props.put(key, ConfigDef.convertToString(values().get(key), typeOf(key))));

    return Collections.unmodifiableMap(props);
  }

  private Map<String, String> getKsqlStreamConfigPropsWithSecretsObfuscated() {
    final Map<String, String> props = new HashMap<>();
    // build a properties map with obfuscated values for sensitive configs.
    // Obfuscation is handled by ConfigDef.convertToString
    ksqlStreamConfigProps.values().stream()
        // we must only return props for which we could resolve
        .filter(ConfigValue::isResolved)
        .forEach(
            configValue -> props.put(
                configValue.key,
                configValue.convertToObfuscatedString()));

    props.putAll(getKsqlStreamTopicConfigProps());
    return Collections.unmodifiableMap(props);
  }

  private Map<String, String> getKsqlStreamTopicConfigProps() {
    final Map<String, String> props = new HashMap<>();

    // Configs with `topic.` prefixes do not appear in the ksqlStreamConfigProps
    // properties, but are considered a streams configuration, i.e.
    // `ksql.streams.topic.min.insync.replicas` (see https://github.com/confluentinc/ksql/pull/3691)
    originalsWithPrefix(KSQL_STREAMS_PREFIX, true).entrySet().stream()
        .filter(e -> e.getKey().startsWith(StreamsConfig.TOPIC_PREFIX))
        .filter(e -> STREAM_TOPIC_CONFIG_NAMES.contains(
            e.getKey().substring(StreamsConfig.TOPIC_PREFIX.length())
        ))
        .forEach(e -> props.put(e.getKey(), e.getValue().toString()));

    return Collections.unmodifiableMap(props);
  }

  public Map<String, String> getAllConfigPropsWithSecretsObfuscated() {
    final Map<String, String> allPropsCleaned = new HashMap<>();
    // build a properties map with obfuscated values for sensitive configs.
    // Obfuscation is handled by ConfigDef.convertToString
    allPropsCleaned.putAll(getKsqlConfigPropsWithSecretsObfuscated());
    allPropsCleaned.putAll(
        getKsqlStreamConfigPropsWithSecretsObfuscated().entrySet().stream().collect(
            Collectors.toMap(
                e -> KSQL_STREAMS_PREFIX + e.getKey(), Map.Entry::getValue
            )
        )
    );
    return Collections.unmodifiableMap(allPropsCleaned);
  }

  public KsqlConfig cloneWithPropertyOverwrite(final Map<String, ?> props) {
    final Map<String, Object> cloneProps = new HashMap<>(originals());
    cloneProps.putAll(props);
    final Map<String, ConfigValue> streamConfigProps =
        buildStreamingConfig(getKsqlStreamConfigProps(), props);

    return new KsqlConfig(ConfigGeneration.CURRENT, cloneProps, streamConfigProps);
  }

  public KsqlConfig overrideBreakingConfigsWithOriginalValues(final Map<String, ?> props) {
    final KsqlConfig originalConfig = new KsqlConfig(ConfigGeneration.LEGACY, props);
    final Map<String, Object> mergedProperties = new HashMap<>(originals());
    COMPATIBLY_BREAKING_CONFIG_DEFS.stream()
        .map(CompatibilityBreakingConfigDef::getName)
        .forEach(
            k -> mergedProperties.put(k, originalConfig.get(k)));
    final Map<String, ConfigValue> mergedStreamConfigProps
        = new HashMap<>(this.ksqlStreamConfigProps);
    COMPATIBILITY_BREAKING_STREAMS_CONFIGS.stream()
        .map(CompatibilityBreakingStreamsConfig::getName)
        .forEach(
            k -> mergedStreamConfigProps.put(k, originalConfig.ksqlStreamConfigProps.get(k)));
    return new KsqlConfig(ConfigGeneration.LEGACY, mergedProperties, mergedStreamConfigProps);
  }

  public Map<String, String> getStringAsMap(final String key) {
    final String value = getString(key).trim();
    return parseStringAsMap(key, value);
  }

  public static Map<String, String> getStringAsMap(
      final String key,
      final Map<String, ?> configMap
  ) {
    final String value = (String) configMap.get(key);
    if (value != null) {
      return parseStringAsMap(key, value);
    } else {
      return Collections.emptyMap();
    }
  }

  public static Map<String, String> parseStringAsMap(final String key, final String value) {
    try {
      return value.equals("")
          ? Collections.emptyMap()
          : Splitter.on(",").trimResults().withKeyValueSeparator(":").split(value);
    } catch (final IllegalArgumentException e) {
      throw new KsqlException(
          String.format(
              "Invalid config value for '%s'. value: %s. reason: %s",
              key,
              value,
              e.getMessage()));
    }
  }

  private static Set<String> sslConfigNames() {
    final ConfigDef sslConfig = new ConfigDef();
    SslConfigs.addClientSslSupport(sslConfig);
    return sslConfig.names();
  }
}
