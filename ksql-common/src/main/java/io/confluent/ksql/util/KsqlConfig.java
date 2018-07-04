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
import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  public static final String DEFAULT_EXT_DIR = "ext";

  private static final Collection<CompatibiltyBreakingConfigDef> COMPATIBILTY_BREAKING_CONFIG_DEFS
      = ImmutableList.of(
          new CompatibiltyBreakingConfigDef(
              KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG,
              ConfigDef.Type.STRING,
              KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT,
              KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT,
              ConfigDef.Importance.MEDIUM,
              "Second part of the prefix for persistent queries. For instance if "
                  + "the prefix is query_ the query name will be ksql_query_1."),
          new CompatibiltyBreakingConfigDef(
              KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG,
              ConfigDef.Type.STRING,
              KSQL_TABLE_STATESTORE_NAME_SUFFIX_DEFAULT,
              KSQL_TABLE_STATESTORE_NAME_SUFFIX_DEFAULT,
              ConfigDef.Importance.MEDIUM,
              "Suffix for state store names in Tables. For instance if the suffix is "
                  + "_ksql_statestore the state "
                  + "store name would be ksql_query_1_ksql_statestore _ksql_statestore "));

  private static class CompatibiltyBreakingConfigDef {
    private final String name;
    private final ConfigDef.Type type;
    private final Object defaultValueOld;
    private final Object defaultValueCurrent;
    private final ConfigDef.Importance importance;
    private final String documentation;

    CompatibiltyBreakingConfigDef(final String name,
                                  final ConfigDef.Type type,
                                  final Object defaultValueOld,
                                  final Object defaultValueCurrent,
                                  final ConfigDef.Importance importance,
                                  final String documentation) {
      this.name = name;
      this.type = type;
      this.defaultValueOld = defaultValueOld;
      this.defaultValueCurrent = defaultValueCurrent;
      this.importance = importance;
      this.documentation = documentation;
    }

    public String getName() {
      return this.name;
    }

    private void define(final ConfigDef configDef, final Object defaultValue) {
      configDef.define(name, type, defaultValue, importance, documentation);
    }

    void defineOld(final ConfigDef configDef) {
      define(configDef, defaultValueOld);
    }

    void defineCurrent(final ConfigDef configDef) {
      define(configDef, defaultValueCurrent);
    }
  }

  private static ConfigDef configDef(final boolean current) {
    final ConfigDef configDef = new ConfigDef()
        .define(
            KSQL_SERVICE_ID_CONFIG,
            ConfigDef.Type.STRING,
            KSQL_SERVICE_ID_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Indicates the ID of the ksql service. It will be used as prefix for "
        )
        .define(
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
    for (final CompatibiltyBreakingConfigDef compatiblityConfigDef
        : COMPATIBILTY_BREAKING_CONFIG_DEFS) {
      if (current) {
        compatiblityConfigDef.defineCurrent(configDef);
      } else {
        compatiblityConfigDef.defineOld(configDef);
      }
    }
    return configDef;
  }

  private static class ConfigValue {
    final Optional<ConfigDef.Type> type;
    final String key;
    final Object value;

    private ConfigValue(final ConfigDef.Type type, final String key, final Object value) {
      this.type = Optional.ofNullable(type);
      this.key = key;
      this.value = value;
    }

    static ConfigValue resolved(final ConfigDef.Type type, final String key, final Object value) {
      return new ConfigValue(type, key, value);
    }

    static ConfigValue unresolved(final String key, final Object value) {
      return new ConfigValue(null, key, value);
    }
  }

  private static ConfigValue resolveConfig(final String prefix,
                                           final AbstractConfig abstractConfig,
                                           final String key,
                                           final Object value) {
    if (!key.startsWith(prefix)) {
      return ConfigValue.unresolved(key, value);
    }
    final String keyNoPrefix = key.substring(prefix.length());
    if (abstractConfig.values().containsKey(keyNoPrefix)) {
      final ConfigDef.Type type = abstractConfig.typeOf(keyNoPrefix);
      return ConfigValue.resolved(type, key, ConfigDef.parseType(keyNoPrefix, value, type));
    }
    return ConfigValue.unresolved(key, value);
  }

  private static final AbstractConfig CONSUMER_ABSTRACT_CONFIG = new ConsumerConfig(
      ImmutableMap.of(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
      )
  );

  private static final AbstractConfig PRODUCER_ABSTRACT_CONFIG = new ProducerConfig(
      ImmutableMap.of(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
      )
  );

  private static final AbstractConfig STREAMS_ABSTRACT_CONFIG = new StreamsConfig(
      ImmutableMap.of(
          StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "",
          StreamsConfig.APPLICATION_ID_CONFIG, ""
      )
  );

  private static Optional<ConfigValue> resolveStreamsConfig(final String maybePrefixedKey,
                                                            final Object value) {
    final String key = maybePrefixedKey.startsWith(KSQL_STREAMS_PREFIX)
        ? maybePrefixedKey.substring(KSQL_STREAMS_PREFIX.length()) : maybePrefixedKey;
    if (key.startsWith(KSQL_CONFIG_PROPERTY_PREFIX)) {
      return Optional.empty();
    }
    final List<Pair<String, AbstractConfig>> configSpecsToTry = ImmutableList.of(
        new Pair<>(StreamsConfig.CONSUMER_PREFIX, CONSUMER_ABSTRACT_CONFIG),
        new Pair<>(StreamsConfig.PRODUCER_PREFIX, PRODUCER_ABSTRACT_CONFIG),
        new Pair<>("", CONSUMER_ABSTRACT_CONFIG),
        new Pair<>("", PRODUCER_ABSTRACT_CONFIG),
        new Pair<>("", STREAMS_ABSTRACT_CONFIG)
    );
    for (Pair<String, AbstractConfig> spec : configSpecsToTry) {
      final ConfigValue configValue
          = resolveConfig(spec.getLeft(), spec.getRight(), key, value);
      if (configValue.type.isPresent()) {
        return Optional.of(configValue);
      }
    }
    return Optional.of(ConfigValue.unresolved(key, value));
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

  private final ImmutableMap<String, ConfigValue> ksqlStreamConfigProps;

  public KsqlConfig(final Map<?, ?> props) {
    this(true, props);
  }

  public KsqlConfig(boolean current, final Map<?, ?> props) {
    super(configDef(current), props);

    final Map<String, Object> streamsConfigDefaults = new HashMap<>();
    streamsConfigDefaults.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KsqlConstants
        .defaultAutoOffsetRestConfig);
    streamsConfigDefaults.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, KsqlConstants
        .defaultCommitIntervalMsConfig);
    streamsConfigDefaults.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, KsqlConstants
            .defaultCacheMaxBytesBufferingConfig);
    streamsConfigDefaults.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, KsqlConstants
        .defaultNumberOfStreamsThreads);
    final Object fail = originals().get(FAIL_ON_DESERIALIZATION_ERROR_CONFIG);
    if (fail == null || !Boolean.parseBoolean(fail.toString())) {
      streamsConfigDefaults.put(
          StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
          LogMetricAndContinueExceptionHandler.class
      );
    }
    final Map<String, ConfigValue> streamConfigProps = new HashMap<>();
    applyStreamsConfig(streamsConfigDefaults, streamConfigProps);
    applyStreamsConfig(originals(), streamConfigProps);
    this.ksqlStreamConfigProps = ImmutableMap.copyOf(streamConfigProps);
  }

  private KsqlConfig(final boolean current,
                     final Map<String, ?> values,
                     final ImmutableMap<String, ConfigValue> ksqlStreamConfigProps) {
    super(configDef(current), values);
    this.ksqlStreamConfigProps = ksqlStreamConfigProps;
  }

  public Map<String, Object> getKsqlStreamConfigProps() {
    final Map<String, Object> props = new HashMap<>();
    for (ConfigValue config : ksqlStreamConfigProps.values()) {
      props.put(config.key, config.value);
    }
    return Collections.unmodifiableMap(props);
  }

  public Map<String, Object> getKsqlAdminClientConfigProps() {
    final Map<String, Object> props = new HashMap<>();
    ksqlStreamConfigProps.values().stream()
        .filter(configValue -> AdminClientConfig.configNames().contains(configValue.key))
        .forEach(
            configValue -> props.put(configValue.key, configValue.value));
    return Collections.unmodifiableMap(props);
  }

  public Map<String, String> getKsqlConfigPropsWithSecretsObfuscated() {
    final Map<String, String> props = new HashMap<>();
    // build a properties map with obfuscated values for sensitive configs.
    // Obfuscation is handled by ConfigDef.convertToString
    configDef(true).names().stream().forEach(
        key -> props.put(key, ConfigDef.convertToString(values().get(key), typeOf(key)))
    );
    return Collections.unmodifiableMap(props);
  }

  public Map<String, String> getKsqlStreamConfigPropsWithSecretsObfuscated() {
    final Map<String, String> props = new HashMap<>();
    // build a properties map with obfuscated values for sensitive configs.
    // Obfuscation is handled by ConfigDef.convertToString
    ksqlStreamConfigProps.values().stream()
        // we must only return props for which we could resolve a type
        .filter(configValue -> configValue.type.isPresent())
        .forEach(
            configValue -> props.put(
                configValue.key,
                ConfigDef.convertToString(configValue.value, configValue.type.get())));
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

  public KsqlConfig clone() {
    return new KsqlConfig(true, values(), ksqlStreamConfigProps);
  }

  public KsqlConfig cloneWithPropertyOverwrite(final Map<String, Object> props) {
    final Map<String, Object> cloneProps = new HashMap<>(values());
    cloneProps.putAll(props);
    final Map<String, ConfigValue> streamConfigProps = new HashMap<>();
    applyStreamsConfig(getKsqlStreamConfigProps(), streamConfigProps);
    applyStreamsConfig(props, streamConfigProps);
    return new KsqlConfig(true, cloneProps, ImmutableMap.copyOf(streamConfigProps));
  }

  public KsqlConfig overrideBreakingConfigsWithOriginalValues(final Map<String, String> props) {
    final KsqlConfig originalConfig = new KsqlConfig(false, props);
    final Map<String, Object> mergedProperties = new HashMap<>(values());
    COMPATIBILTY_BREAKING_CONFIG_DEFS.stream()
        .map(CompatibiltyBreakingConfigDef::getName)
        .forEach(
            k -> mergedProperties.put(k, originalConfig.get(k)));
    return new KsqlConfig(true, mergedProperties, ksqlStreamConfigProps);
  }

  /* 6/19/2018: Temporary hack to pass around the timestamp column for a query */

  private int timestampColumnIndex = -1;

  public void setKsqlTimestampColumnIndex(int index) {
    this.timestampColumnIndex = index;
  }

  public int getKsqlTimestampColumnIndex() {
    return this.timestampColumnIndex;
  }

  /* end hack */
}
