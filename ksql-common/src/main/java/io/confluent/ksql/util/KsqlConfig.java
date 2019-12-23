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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.ConfigItem;
import io.confluent.ksql.config.KsqlConfigResolver;
import io.confluent.ksql.errors.LogMetricAndContinueExceptionHandler;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil;
import io.confluent.ksql.model.SemanticVersion;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.streams.StreamsConfig;

@EffectivelyImmutable
public class KsqlConfig extends AbstractConfig {

  public static final String KSQL_CONFIG_PROPERTY_PREFIX = "ksql.";

  public static final String KSQL_FUNCTIONS_PROPERTY_PREFIX =
      KSQL_CONFIG_PROPERTY_PREFIX + "functions.";

  static final String KSQ_FUNCTIONS_GLOBAL_PROPERTY_PREFIX =
      KSQL_FUNCTIONS_PROPERTY_PREFIX + "_global_.";

  public static final String METRIC_REPORTER_CLASSES_CONFIG = "ksql.metric.reporters";

  public static final String METRIC_REPORTER_CLASSES_DOC =
      CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC;

  public static final String KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY = "ksql.internal.topic.replicas";

  public static final String KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS_PROPERTY = 
      "ksql.internal.topic.min.insync.replicas";

  public static final String KSQL_SCHEMA_REGISTRY_PREFIX = "ksql.schema.registry.";

  public static final String SCHEMA_REGISTRY_URL_PROPERTY = "ksql.schema.registry.url";

  public static final String CONNECT_URL_PROPERTY = "ksql.connect.url";

  public static final String CONNECT_WORKER_CONFIG_FILE_PROPERTY = "ksql.connect.worker.config";

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

  public static final String KSQL_WRAP_SINGLE_VALUES =
      "ksql.persistence.wrap.single.values";

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

  public static final String KSQL_ENABLE_TOPIC_ACCESS_VALIDATOR = "ksql.access.validator.enable";
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
  public static final String KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_CONFIG =
      "ksql.query.pull.skip.access.validator";
  public static final boolean KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_DEFAULT = false;
  public static final String KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_DOC = "If \"true\", KSQL will "
      + " NOT enforce access validation checks for pull queries, which could expose Kafka topics"
      + " which are secured with ACLs. Please enable only after careful consideration."
      + " If \"false\", KSQL pull queries will fail against a secure Kafka cluster";

  public static final String KSQL_PULL_QUERIES_ENABLE_CONFIG = "ksql.pull.queries.enable";
  public static final String KSQL_QUERY_PULL_ENABLE_DOC =
      "Config to enable or disable transient pull queries on a specific KSQL server.";
  public static final boolean KSQL_QUERY_PULL_ENABLE_DEFAULT = true;

  public static final String KSQL_QUERY_PULL_ROUTING_TIMEOUT_MS_CONFIG =
      "ksql.query.pull.routing.timeout.ms";
  public static final Long KSQL_QUERY_PULL_ROUTING_TIMEOUT_MS_DEFAULT = 30000L;
  public static final String KSQL_QUERY_PULL_ROUTING_TIMEOUT_MS_DOC = "Timeout in milliseconds "
      + "when waiting for the lookup of the owner of a row key";

  public static final String KSQL_QUERY_PULL_STREAMSTORE_REBALANCING_TIMEOUT_MS_CONFIG =
      "ksql.query.pull.streamsstore.rebalancing.timeout.ms";
  public static final Long KSQL_QUERY_PULL_STREAMSTORE_REBALANCING_TIMEOUT_MS_DEFAULT = 10000L;
  public static final String KSQL_QUERY_PULL_STREAMSTORE_REBALANCING_TIMEOUT_MS_DOC = "Timeout in "
      + "milliseconds when waiting for rebalancing of the stream store during a pull query";

  public static final String KSQL_EXECUTION_PLANS_ENABLE = "ksql.execution.plan.enable";
  public static final boolean KSQL_EXECUTION_PLANS_ENABLE_DEFAULT = false;

  public static final Collection<CompatibilityBreakingConfigDef> COMPATIBLY_BREAKING_CONFIG_DEFS
      = ImmutableList.of();

  public static final String KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG =
      "ksql.streams.shutdown.timeout.ms";
  public static final Long KSQL_SHUTDOWN_TIMEOUT_MS_DEFAULT = 300_000L;
  public static final String KSQL_SHUTDOWN_TIMEOUT_MS_DOC = "Timeout in "
      + "milliseconds to block waiting for the underlying streams instance to exit";

  public static final String KSQL_AUTH_CACHE_EXPIRY_TIME =
      "ksql.authorization.cache.expiry.time";
  public static final Long KSQL_AUTH_CACHE_EXPIRY_TIME_DEFAULT = 30L;
  public static final String KSQL_AUTH_CACHE_EXPIRY_TIME_DOC = "Time in "
      + "seconds to keep KSQL authorization responses in the cache.";

  public static final String KSQL_AUTH_CACHE_MAX_ENTRIES =
      "ksql.authorization.cache.max.entries";
  public static final Long KSQL_AUTH_CACHE_MAX_ENTRIES_DEFAULT = 10000L;
  public static final String KSQL_AUTH_CACHE_MAX_ENTRIES_DOC = "Controls the size of the cache "
      + "to a maximum number of KSQL authorization responses entries.";

  private enum ConfigGeneration {
    LEGACY,
    CURRENT
  }

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
            StreamsConfig.TOPOLOGY_OPTIMIZATION,
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

  private static ConfigDef configDef(final ConfigGeneration generation) {
    return generation == ConfigGeneration.CURRENT ? CURRENT_DEF : LEGACY_DEF;
  }

  // CHECKSTYLE_RULES.OFF: MethodLength
  private static ConfigDef buildConfigDef(final ConfigGeneration generation) {
    final ConfigDef configDef = new ConfigDef()
        .define(
            KSQL_SERVICE_ID_CONFIG,
            ConfigDef.Type.STRING,
            KSQL_SERVICE_ID_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Indicates the ID of the ksql service. It will be used as prefix for "
                + "all implicitly named resources created by this instance in Kafka. "
                + "By convention, the id should end in a seperator character of some form, e.g. "
                + "a dash or underscore, as this makes identifiers easier to read."
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
            DEFAULT_SCHEMA_REGISTRY_URL,
            ConfigDef.Importance.MEDIUM,
            "The URL for the schema registry, defaults to http://localhost:8081"
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
            KSQL_WRAP_SINGLE_VALUES,
            ConfigDef.Type.BOOLEAN,
            true,
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
            KSQL_CUSTOM_METRICS_EXTENSION,
            ConfigDef.Type.CLASS,
            null,
            ConfigDef.Importance.LOW,
            KSQL_CUSTOM_METRICS_EXTENSION_DOC
        ).define(
            KSQL_ENABLE_TOPIC_ACCESS_VALIDATOR,
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
            KSQL_QUERY_PULL_ROUTING_TIMEOUT_MS_CONFIG,
            ConfigDef.Type.LONG,
            KSQL_QUERY_PULL_ROUTING_TIMEOUT_MS_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PULL_ROUTING_TIMEOUT_MS_DOC
        ).define(
            KSQL_QUERY_PULL_STREAMSTORE_REBALANCING_TIMEOUT_MS_CONFIG,
            ConfigDef.Type.LONG,
            KSQL_QUERY_PULL_STREAMSTORE_REBALANCING_TIMEOUT_MS_DEFAULT,
            Importance.LOW,
            KSQL_QUERY_PULL_STREAMSTORE_REBALANCING_TIMEOUT_MS_DOC
        ).define(
            KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_CONFIG,
            Type.BOOLEAN,
            KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_DEFAULT,
            Importance.LOW,
            KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_DOC
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
            KSQL_EXECUTION_PLANS_ENABLE,
            Type.BOOLEAN,
            KSQL_EXECUTION_PLANS_ENABLE_DEFAULT,
            Importance.LOW,
            "Feature flag to enable writing KSQL execution plans. For testing only."
        ).define(
            KSQL_AUTH_CACHE_EXPIRY_TIME,
            Type.LONG,
            KSQL_AUTH_CACHE_EXPIRY_TIME_DEFAULT,
            Importance.LOW,
            KSQL_AUTH_CACHE_EXPIRY_TIME_DOC
        ).define(
            KSQL_AUTH_CACHE_MAX_ENTRIES,
            Type.LONG,
            KSQL_AUTH_CACHE_MAX_ENTRIES_DEFAULT,
            Importance.LOW,
            KSQL_AUTH_CACHE_MAX_ENTRIES_DOC
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
  // CHECKSTYLE_RULES.ON: MethodLength

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

  public Map<String, Object> getKsqlStreamConfigProps() {
    final Map<String, Object> props = new HashMap<>();
    for (final ConfigValue config : ksqlStreamConfigProps.values()) {
      props.put(config.key, config.value);
    }
    return Collections.unmodifiableMap(props);
  }

  public Map<String, Object> getKsqlAdminClientConfigProps() {
    return getConfigsFor(AdminClientConfig.configNames());
  }

  public Map<String, Object> getProducerClientConfigProps() {
    return getConfigsFor(ProducerConfig.configNames());
  }

  private Map<String, Object> getConfigsFor(final Set<String> configs) {
    final Map<String, Object> props = new HashMap<>();
    ksqlStreamConfigProps.values().stream()
        .filter(configValue -> configs.contains(configValue.key))
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
