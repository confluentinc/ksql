/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.confluent.ksql.errors.LogMetricAndContinueExceptionHandler;

public class KsqlConfig extends AbstractConfig implements Cloneable {

  public static final String KSQL_CONFIG_PROPERTY_PREFIX = "ksql.";

  public static final String SINK_NUMBER_OF_PARTITIONS_PROPERTY = "ksql.sink.partitions";

  public static final String SINK_NUMBER_OF_REPLICAS_PROPERTY = "ksql.sink.replicas";

  public static final String KSQL_SCHEMA_REGISTRY_PREFIX = "ksql.schema.registry.";

  public static final String SCHEMA_REGISTRY_URL_PROPERTY = "ksql.schema.registry.url";

  public static final String KSQL_ENABLE_UDFS = "ksql.udfs.enabled";

  public static final String KSQL_EXT_DIR = "ksql.extension.dir";

  public static final String SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY =
      "ksql.sink.window.change.log.additional.retention";

  public static final String
      FAIL_ON_DESERIALIZATION_ERROR_CONFIG = "ksql.fail.on.deserialization.error";

  public static final String
      KSQL_SERVICE_ID_CONFIG = "ksql.service.id";
  public static final String
      KSQL_SERVICE_ID_DEFAULT = "default_";

  public static final String
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG = "ksql.persistent.prefix";
  public static final String
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT = "query_";

  public static final String
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG = "ksql.transient.prefix";
  public static final String
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT = "transient_";

  public static final String
      KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG = "ksql.statestore.suffix";
  public static final String
      KSQL_TABLE_STATESTORE_NAME_SUFFIX_DEFAULT = "_ksql_statestore";

  public static final String
      KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG = "ksql.output.topic.name.prefix";
  private static final String KSQL_OUTPUT_TOPIC_NAME_PREFIX_DOCS =
      "A prefix to add to any output topic names, where the statement does not include an explicit "
      + "topic name. E.g. given 'ksql.output.topic.name.prefix = \"thing-\"', then statement "
      + "'CREATE STREAM S AS ...' will create a topic 'thing-S', where as the statement "
      + "'CREATE STREAM S WITH(KAFKA_TOPIC = 'foo') AS ...' will create a topic 'foo'.";

  public static final String
      defaultSchemaRegistryUrl = "http://localhost:8081";

  public static final String KSQL_STREAMS_PREFIX = "ksql.streams.";

  public static final String KSQL_COLLECT_UDF_METRICS = "ksql.udf.collect.metrics";
  public static final String KSQL_UDF_SECURITY_MANAGER_ENABLED = "ksql.udf.enable.security.manager";

  private final ImmutableMap<String, Object> ksqlStreamConfigProps;

  private static final ConfigDef CONFIG_DEF;

  public static final String DEFAULT_EXT_DIR = "ext";

  static {
    CONFIG_DEF = new ConfigDef()
        .define(
            KSQL_SERVICE_ID_CONFIG,
            ConfigDef.Type.STRING,
            KSQL_SERVICE_ID_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Indicates the ID of the ksql service. It will be used as prefix for "
            + "all KSQL queries in this service."
        ).define(
            KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Second part of the prefix for persistent queries. For instance if "
            + "the prefix is query_ the query name will be ksql_query_1."
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
            KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG,
            ConfigDef.Type.STRING,
            KSQL_TABLE_STATESTORE_NAME_SUFFIX_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Suffix for state store names in Tables. For instance if the suffix is "
            + "_ksql_statestore the state "
            + "store name would be ksql_query_1_ksql_statestore _ksql_statestore "
        ).define(
            KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.LOW,
            KSQL_OUTPUT_TOPIC_NAME_PREFIX_DOCS
        ).define(
            SINK_NUMBER_OF_PARTITIONS_PROPERTY,
            ConfigDef.Type.INT,
            KsqlConstants.defaultSinkNumberOfPartitions,
            ConfigDef.Importance.MEDIUM,
            "The default number of partitions for the topics created by KSQL."
        ).define(
            SINK_NUMBER_OF_REPLICAS_PROPERTY,
            ConfigDef.Type.SHORT,
            KsqlConstants.defaultSinkNumberOfReplications,
            ConfigDef.Importance.MEDIUM,
            "The default number of replicas for the topics created by KSQL."
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
            defaultSchemaRegistryUrl,
            ConfigDef.Importance.MEDIUM,
            "The URL for the schema registry, defaults to http://localhost:8081"
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
            KSQL_UDF_SECURITY_MANAGER_ENABLED,
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.LOW,
            "Enable the security manager for UDFs. Default is true and will stop UDFs from"
               + " calling System.exit or executing processes"
        )

        .withClientSslSupport();
  }

  private static void applyPrefixedConfig(final Map<String, Object> props,
                                          final String prefix,
                                          final String key,
                                          final Object value) {
    props.put(
        key.startsWith(prefix) ? key.substring(prefix.length()) : key,
        value
    );
  }

  private static boolean streamsConfigFilter(final String config) {
    if (config.startsWith(KSQL_STREAMS_PREFIX)) {
      return true;
    }
    return StreamsConfig.configDef().names().contains(config)
        || ConsumerConfig.configNames().contains(config)
        || ProducerConfig.configNames().contains(config)
        || AdminClientConfig.configNames().contains(config);
  }

  private static void applyStreamsConfig(final Map<String, Object> props,
                                         final Map<String, Object> streamsConfigProps) {
    props.entrySet()
        .stream()
        .map(Map.Entry::getKey)
        .filter(KsqlConfig::streamsConfigFilter)
        .forEach(
            k -> applyPrefixedConfig(streamsConfigProps, KSQL_STREAMS_PREFIX, k, props.get(k)));
  }

  public KsqlConfig(final Map<?, ?> props) {
    super(CONFIG_DEF, props);

    final Map<String, Object> streamsConfigOverlay = new HashMap<>();

    streamsConfigOverlay.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KsqlConstants
        .defaultAutoOffsetRestConfig);
    streamsConfigOverlay.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, KsqlConstants
        .defaultCommitIntervalMsConfig);
    streamsConfigOverlay.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, KsqlConstants
            .defaultCacheMaxBytesBufferingConfig);
    streamsConfigOverlay.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, KsqlConstants
        .defaultNumberOfStreamsThreads);

    final Object fail = originals().get(FAIL_ON_DESERIALIZATION_ERROR_CONFIG);
    if (fail == null || !Boolean.parseBoolean(fail.toString())) {
      streamsConfigOverlay.put(
          StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
          LogMetricAndContinueExceptionHandler.class
      );
    }

    applyStreamsConfig(originals(), streamsConfigOverlay);
    this.ksqlStreamConfigProps = ImmutableMap.copyOf(streamsConfigOverlay);
  }

  private KsqlConfig(final Map<String, ?> values,
                     final ImmutableMap<String, Object> ksqlStreamConfigProps) {
    super(CONFIG_DEF, values);
    this.ksqlStreamConfigProps = ksqlStreamConfigProps;
  }

  public ImmutableMap<String, Object> getKsqlStreamConfigProps() {
    return ksqlStreamConfigProps;
  }

  public ImmutableMap<String, Object> getKsqlAdminClientConfigProps() {
    return ksqlStreamConfigProps.entrySet().stream()
        .map(Map.Entry::getKey)
        .filter(AdminClientConfig.configNames()::contains)
        .collect(
            ImmutableMap.toImmutableMap(name -> name, ksqlStreamConfigProps::get)
        );
  }

  public ImmutableMap<String, Object> getAllProps() {
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.putAll(values().entrySet().stream()
        .filter(e -> e.getValue() != null)
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue))
    );
    builder.putAll(ksqlStreamConfigProps.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .collect(
            Collectors.toMap(
                e -> KSQL_STREAMS_PREFIX + e.getKey(),
                Map.Entry::getValue)));
    return builder.build();
  }

  public KsqlConfig clone() {
    return new KsqlConfig(values(), ksqlStreamConfigProps);
  }

  public KsqlConfig cloneWithPropertyOverwrite(final Map<String, Object> props) {
    final Map<String, Object> cloneProps = new HashMap<>(values());
    cloneProps.putAll(props);
    final Map<String, Object> streamsConfigOverlay = new HashMap<>();
    applyStreamsConfig(ksqlStreamConfigProps, streamsConfigOverlay);
    applyStreamsConfig(props, streamsConfigOverlay);
    return new KsqlConfig(cloneProps, ImmutableMap.copyOf(streamsConfigOverlay));
  }

  /* 6/19/2018: Temporary hack to pass around the timestamp column for a query
                When this is removed KsqlConfig becomes immutable(ish)
                and hence the clone method should be removed.
   */

  private int timestampColumnIndex = -1;

  public void setKsqlTimestampColumnIndex(int index) {
    this.timestampColumnIndex = index;
  }

  public int getKsqlTimestampColumnIndex() {
    return this.timestampColumnIndex;
  }

  /* end hack */
}
