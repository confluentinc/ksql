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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.confluent.ksql.errors.LogMetricAndContinueExceptionHandler;

public class KsqlConfig extends AbstractConfig implements Cloneable {

  public static final String KSQL_CONFIG_PROPERTY_PREFIX = "ksql.";

  public static final String KSQL_TIMESTAMP_COLUMN_INDEX = "ksql.timestamp.column.index";

  public static final String STRING_TIMESTAMP_FORMAT = "ksq.timestamp.string.format";

  public static final String SINK_NUMBER_OF_PARTITIONS_PROPERTY = "ksql.sink.partitions";

  public static final String SINK_NUMBER_OF_REPLICAS_PROPERTY = "ksql.sink.replicas";

  public static final String KSQL_SCHEMA_REGISTRY_PREFIX = "ksql.schema.registry.";

  public static final String SCHEMA_REGISTRY_URL_PROPERTY = "ksql.schema.registry.url";

  public static final String SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY =
      "ksql.sink.window.change.log.additional.retention";

  public static final String STREAM_INTERNAL_CHANGELOG_TOPIC_SUFFIX = "-changelog";

  public static final String STREAM_INTERNAL_REPARTITION_TOPIC_SUFFIX = "-repartition";

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
      defaultSchemaRegistryUrl = "http://localhost:8081";

  public static final boolean defaultAvroSchemaUnionNull = true;
  public static final String KSQL_STREAMS_PREFIX = "ksql.streams.";

  private final Map<String, Object> ksqlConfigProps;
  private final Map<String, Object> ksqlStreamConfigProps;

  private static final ConfigDef CONFIG_DEF;

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
            "Second part of the prefix for persitent queries. For instance if "
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
        ).define(KSQL_TIMESTAMP_COLUMN_INDEX,
            ConfigDef.Type.INT,
            null,
            ConfigDef.Importance.LOW,
            "The index of the timestamp column for a specific ksql query. Only present if"
                + "the query has specified a timestamp in the WITH(..) clause."
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
        ).withClientSslSupport();
  }

  private static Map<String, Object> commonConfigs(Map<String, Object> props) {
    return props.entrySet().stream()
        .filter(e -> !e.getKey().startsWith(KSQL_CONFIG_PROPERTY_PREFIX))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static Map<String, Object> propertiesWithPrefix(
      Map<String, Object> props, String prefix) {
    AbstractConfig abstractConfig = new AbstractConfig(new ConfigDef(), props);
    return abstractConfig.originalsWithPrefix(prefix);
  }


  private void applyStreamsConfig(Map<String, Object> props) {
    ksqlStreamConfigProps.putAll(commonConfigs(props));
    ksqlStreamConfigProps.putAll(propertiesWithPrefix(props, KSQL_STREAMS_PREFIX));
  }

  public KsqlConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);

    ksqlConfigProps = new HashMap<>();
    ksqlStreamConfigProps = new HashMap<>();

    ksqlConfigProps.putAll(this.values());

    ksqlStreamConfigProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KsqlConstants
        .defaultAutoOffsetRestConfig);
    ksqlStreamConfigProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, KsqlConstants
        .defaultCommitIntervalMsConfig);
    ksqlStreamConfigProps.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, KsqlConstants
            .defaultCacheMaxBytesBufferingConfig);
    ksqlStreamConfigProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, KsqlConstants
        .defaultNumberOfStreamsThreads);

    final Object fail = originals().get(FAIL_ON_DESERIALIZATION_ERROR_CONFIG);
    if (fail == null || !Boolean.parseBoolean(fail.toString())) {
      ksqlStreamConfigProps.put(
          StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
          LogMetricAndContinueExceptionHandler.class
      );
    }

    applyStreamsConfig(originals());
  }

  public Map<String, Object> getKsqlConfigProps() {
    return ksqlConfigProps;
  }

  public Map<String, Object> getKsqlStreamConfigProps() {
    return ksqlStreamConfigProps;
  }

  public Map<String, Object> getKsqlAdminClientConfigProps() {
    final Map<String, Object> adminClientConfigs = new HashMap<>(ksqlStreamConfigProps);
    adminClientConfigs.keySet().retainAll(AdminClientConfig.configNames());
    return adminClientConfigs;
  }

  public Object get(String propertyName) {
    if (propertyName.toLowerCase().startsWith(KSQL_CONFIG_PROPERTY_PREFIX)) {
      return ksqlConfigProps.get(propertyName);
    } else {
      return ksqlStreamConfigProps.get(propertyName);
    }
  }

  public void put(String propertyName, Object propertyValue) {
    if (propertyName.toLowerCase().startsWith(KSQL_CONFIG_PROPERTY_PREFIX)) {
      ksqlConfigProps.put(propertyName, propertyValue);
    } else if (propertyName.startsWith(KSQL_STREAMS_PREFIX)) {
      ksqlStreamConfigProps.put(
          propertyName.substring(KSQL_STREAMS_PREFIX.length()), propertyValue);
    } else {
      ksqlStreamConfigProps.put(propertyName, propertyValue);
    }
  }

  public KsqlConfig clone() {
    Map<String, Object> clonedProperties = new HashMap<>();
    clonedProperties.putAll(originals());
    return new KsqlConfig(clonedProperties);
  }

  public KsqlConfig cloneWithPropertyOverwrite(final Map<String, Object> props) {
    final Map<String, Object> cloned = new HashMap<>();

    cloned.putAll(ksqlConfigProps.entrySet().stream()
                      .filter(e -> e.getValue() != null)
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    cloned.putAll(ksqlStreamConfigProps.entrySet().stream()
                      .filter(e -> e.getValue() != null)
                      .collect(Collectors.toMap(
                          e -> KSQL_STREAMS_PREFIX + e.getKey(),
                          Map.Entry::getValue)));

    cloned.putAll(props);

    final KsqlConfig clone = new KsqlConfig(cloned);
    // re-apply streams configs so that any un-prefixed overwrite settings
    // take precedence over older prefixed settings
    clone.applyStreamsConfig(props);
    return clone;
  }
}
