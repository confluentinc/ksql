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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.config.ConfigItem;
import io.confluent.ksql.config.KsqlConfigResolver;
import io.confluent.ksql.errors.LogMetricAndContinueExceptionHandler;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.streams.StreamsConfig;

public class KsqlConfig extends AbstractConfig implements Cloneable {

  public static final String KSQL_CONFIG_PROPERTY_PREFIX = "ksql.";

  public static final String KSQL_FUNCTIONS_PROPERTY_PREFIX =
      KSQL_CONFIG_PROPERTY_PREFIX + "functions.";

  static final String KSQ_FUNCTIONS_GLOBAL_PROPERTY_PREFIX =
      KSQL_FUNCTIONS_PROPERTY_PREFIX + "_global_.";

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

  public static final String KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS_CONFIG =
      KSQL_FUNCTIONS_PROPERTY_PREFIX + "substring.legacy.args";
  private static final String
      KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS_DOCS = "Switch the SUBSTRING function into legacy mode,"
      + " i.e. back to how it was in version 5.0 and earlier of KSQL."
      + " Up to version 5.0.x substring took different args:"
      + " VARCHAR SUBSTRING(str VARCHAR, startIndex INT, endIndex INT), where startIndex and"
      + " endIndex were both base-zero indexed, e.g. a startIndex of '0' selected the start of the"
      + " string, and the last argument is a character index, rather than the length of the"
      + " substring to extract. Later versions of KSQL use:"
      + " VARCHAR SUBSTRING(str VARCHAR, pos INT, length INT), where pos is base-one indexed,"
      + " and the last argument is the length of the substring to extract.";

  public static final String KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG =
      KSQL_CONFIG_PROPERTY_PREFIX + "windowed.session.key.legacy";

  private static final String KSQL_WINDOWED_SESSION_KEY_LEGACY_DOC = ""
      + "Version 5.1 of KSQL and earlier incorrectly excluded the end time in the record key in "
      + "Kafka for session windowed data. Setting this value to true will make KSQL expect and "
      + "continue to store session keys without the end time. With the default value of false "
      + "new queries will now correctly store the session end time as part of the key";

  public static final String KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG =
      "ksql.query.persistent.active.limit";
  private static final int KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DEFAULT = Integer.MAX_VALUE;
  private static final String KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DOC =
      "An upper limit on the number of active, persistent queries that may be running at a time, "
      + "in interactive mode. Once this limit is reached, any further persistent queries will not "
      + "be accepted.";

  public static final String KSQL_USE_NAMED_INTERNAL_TOPICS = "ksql.named.internal.topics";
  private static final String KSQL_USE_NAMED_INTERNAL_TOPICS_DOC = "";
  public static final String KSQL_USE_NAMED_INTERNAL_TOPICS_ON = "on";
  public static final String KSQL_USE_NAMED_INTERNAL_TOPICS_OFF = "off";
  private static final Validator KSQL_USE_NAMED_INTERNAL_TOPICS_VALIDATOR = ValidString.in(
      KSQL_USE_NAMED_INTERNAL_TOPICS_ON, KSQL_USE_NAMED_INTERNAL_TOPICS_OFF
  );

  public static final String
      defaultSchemaRegistryUrl = "http://localhost:8081";

  public static final String KSQL_STREAMS_PREFIX = "ksql.streams.";

  public static final String KSQL_COLLECT_UDF_METRICS = "ksql.udf.collect.metrics";
  public static final String KSQL_UDF_SECURITY_MANAGER_ENABLED = "ksql.udf.enable.security.manager";

  public static final String DEFAULT_EXT_DIR = "ext";

  private static final Collection<CompatibilityBreakingConfigDef> COMPATIBLY_BREAKING_CONFIG_DEFS
      = ImmutableList.of(
          new CompatibilityBreakingConfigDef(
              KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG,
              ConfigDef.Type.STRING,
              KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT,
              KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT,
              ConfigDef.Importance.MEDIUM,
              "Second part of the prefix for persistent queries. For instance if "
                  + "the prefix is query_ the query name will be ksql_query_1."),
          new CompatibilityBreakingConfigDef(
              KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG,
              ConfigDef.Type.STRING,
              KSQL_TABLE_STATESTORE_NAME_SUFFIX_DEFAULT,
              KSQL_TABLE_STATESTORE_NAME_SUFFIX_DEFAULT,
              ConfigDef.Importance.MEDIUM,
              "Suffix for state store names in Tables. For instance if the suffix is "
                  + "_ksql_statestore the state "
                  + "store name would be ksql_query_1_ksql_statestore _ksql_statestore "),
          new CompatibilityBreakingConfigDef(
              KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS_CONFIG,
              ConfigDef.Type.BOOLEAN,
              true,
              false,
              ConfigDef.Importance.LOW,
              KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS_DOCS),
          new CompatibilityBreakingConfigDef(
              KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG,
              ConfigDef.Type.BOOLEAN,
              true,
              false,
              ConfigDef.Importance.LOW,
              KSQL_WINDOWED_SESSION_KEY_LEGACY_DOC),
          new CompatibilityBreakingConfigDef(
              KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG,
              ConfigDef.Type.INT,
              KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DEFAULT,
              KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DEFAULT,
              ConfigDef.Importance.LOW,
              KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DOC),
          new CompatibilityBreakingConfigDef(
              KSQL_USE_NAMED_INTERNAL_TOPICS,
              ConfigDef.Type.STRING,
              KSQL_USE_NAMED_INTERNAL_TOPICS_OFF,
              KSQL_USE_NAMED_INTERNAL_TOPICS_ON,
              ConfigDef.Importance.LOW,
              KSQL_USE_NAMED_INTERNAL_TOPICS_DOC,
              KSQL_USE_NAMED_INTERNAL_TOPICS_VALIDATOR)
  );

  private enum ConfigGeneration {
    LEGACY,
    CURRENT
  }

  private static class CompatibilityBreakingConfigDef {
    private final String name;
    private final ConfigDef.Type type;
    private final Object defaultValueLegacy;
    private final Object defaultValueCurrent;
    private final ConfigDef.Importance importance;
    private final String documentation;
    private final Validator validator;

    CompatibilityBreakingConfigDef(final String name,
        final ConfigDef.Type type,
        final Object defaultValueLegacy,
        final Object defaultValueCurrent,
        final ConfigDef.Importance importance,
        final String documentation) {
      this(name, type, defaultValueLegacy, defaultValueCurrent, importance, documentation, null);
    }

    CompatibilityBreakingConfigDef(
        final String name,
        final ConfigDef.Type type,
        final Object defaultValueLegacy,
        final Object defaultValueCurrent,
        final ConfigDef.Importance importance,
        final String documentation,
        final Validator validator) {
      this.name = name;
      this.type = type;
      this.defaultValueLegacy = defaultValueLegacy;
      this.defaultValueCurrent = defaultValueCurrent;
      this.importance = importance;
      this.documentation = documentation;
      this.validator = validator;
    }

    public String getName() {
      return this.name;
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
          new CompatibilityBreakingStreamsConfig(
              StreamsConfig.TOPOLOGY_OPTIMIZATION,
              StreamsConfig.NO_OPTIMIZATION,
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

  private static ConfigDef configDef(final ConfigGeneration generation) {
    return generation == ConfigGeneration.CURRENT ? CURRENT_DEF : LEGACY_DEF;
  }

  private static ConfigDef buildConfigDef(final ConfigGeneration generation) {
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
    streamsConfigDefaults.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KsqlConstants
        .defaultAutoOffsetRestConfig);
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
    this.ksqlStreamConfigProps = buildStreamingConfig(streamsConfigDefaults, originals());
    validate();
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

  private void validate() {
    final Object optimizationsConfig = getKsqlStreamConfigProps().get(
        StreamsConfig.TOPOLOGY_OPTIMIZATION);
    final Object useInternalNamesConfig = get(KSQL_USE_NAMED_INTERNAL_TOPICS);
    if (Objects.equals(optimizationsConfig, StreamsConfig.OPTIMIZE)
        && useInternalNamesConfig.equals(KSQL_USE_NAMED_INTERNAL_TOPICS_OFF)) {
      throw new RuntimeException(
          "Internal topic naming must be enabled if streams optimizations enabled");
    }
  }

  public Map<String, Object> getKsqlStreamConfigProps() {
    final Map<String, Object> props = new HashMap<>();
    for (final ConfigValue config : ksqlStreamConfigProps.values()) {
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

  @SuppressFBWarnings("CN_IDIOM_NO_SUPER_CALL")
  public KsqlConfig clone() {
    return new KsqlConfig(ConfigGeneration.CURRENT, values(), ksqlStreamConfigProps);
  }

  public KsqlConfig cloneWithPropertyOverwrite(final Map<String, Object> props) {
    final Map<String, Object> cloneProps = new HashMap<>(values());
    cloneProps.putAll(props);
    final Map<String, ConfigValue> streamConfigProps =
        buildStreamingConfig(getKsqlStreamConfigProps(), props);

    return new KsqlConfig(ConfigGeneration.CURRENT, cloneProps, streamConfigProps);
  }

  public KsqlConfig overrideBreakingConfigsWithOriginalValues(final Map<String, String> props) {
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

  private static Set<String> sslConfigNames() {
    final ConfigDef sslConfig = new ConfigDef();
    SslConfigs.addClientSslSupport(sslConfig);
    return sslConfig.names();
  }
}
