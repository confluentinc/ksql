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

package io.confluent.ksql.config;

import static io.confluent.ksql.util.KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static io.confluent.ksql.util.KsqlRequestConfig.KSQL_REQUEST_CONFIG_PROPERTY_PREFIX;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlRequestConfig;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;

/**
 * Resolves Ksql and streams property name to a ConfigItem.
 */
public class KsqlConfigResolver implements ConfigResolver {

  private static final ConfigDef STREAMS_CONFIG_DEF = StreamsConfig.configDef();
  private static final ConfigDef CONSUMER_CONFIG_DEF = getConfigDef(ConsumerConfig.class);
  private static final ConfigDef PRODUCER_CONFIG_DEF = getConfigDef(ProducerConfig.class);
  private static final ConfigDef KSQL_CONFIG_DEF = KsqlConfig.CURRENT_DEF;
  private static final ConfigDef REQUEST_CONFIG_DEF = KsqlRequestConfig.CURRENT_DEF;

  private static final List<PrefixedConfig> STREAM_CONFIG_DEFS = ImmutableList.of(
      new PrefixedConfig(StreamsConfig.CONSUMER_PREFIX, CONSUMER_CONFIG_DEF),
      new PrefixedConfig(StreamsConfig.PRODUCER_PREFIX, PRODUCER_CONFIG_DEF),
      new PrefixedConfig("", CONSUMER_CONFIG_DEF),
      new PrefixedConfig("", PRODUCER_CONFIG_DEF),
      new PrefixedConfig("", STREAMS_CONFIG_DEF)
  );

  @Override
  public  Optional<ConfigItem> resolve(final String propertyName, final boolean strict) {
    if (propertyName.startsWith(KSQL_REQUEST_CONFIG_PROPERTY_PREFIX)) {
      return resolveRequestConfig(propertyName);
    } else if (propertyName.startsWith(KSQL_CONFIG_PROPERTY_PREFIX)
        && !propertyName.startsWith(KSQL_STREAMS_PREFIX)) {
      return resolveKsqlConfig(propertyName);
    }

    return resolveStreamsConfig(propertyName, strict);
  }

  private static Optional<ConfigItem> resolveStreamsConfig(
      final String propertyName,
      final boolean strict) {

    final String key = stripPrefix(propertyName, KSQL_STREAMS_PREFIX);

    final Optional<ConfigItem> resolved = STREAM_CONFIG_DEFS
        .stream()
        .map(def -> resolveConfig(def.prefix, def.def, key))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();

    if (resolved.isPresent()) {
      return resolved;
    }

    if (propertyName.startsWith(KSQL_STREAMS_PREFIX)
        && !propertyName.startsWith(KSQL_STREAMS_PREFIX + StreamsConfig.PRODUCER_PREFIX)
        && !propertyName.startsWith(KSQL_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX)
        && !propertyName.startsWith(KSQL_STREAMS_PREFIX + StreamsConfig.TOPIC_PREFIX)) {
      return Optional.empty();  // Unknown streams config
    }

    if (key.startsWith(StreamsConfig.TOPIC_PREFIX)) {
      final String topicKey = stripPrefix(key, StreamsConfig.TOPIC_PREFIX);
      if (KsqlConfig.STREAM_TOPIC_CONFIG_NAMES.contains(topicKey)) {
        return Optional.of(ConfigItem.unresolved(key));
      }
    }

    // Unknown config (which could be used):
    return strict ? Optional.empty() : Optional.of(ConfigItem.unresolved(key));
  }

  private static Optional<ConfigItem> resolveKsqlConfig(final String propertyName) {
    final Optional<ConfigItem> possibleItem = resolveConfig("", KSQL_CONFIG_DEF, propertyName);
    if (possibleItem.isPresent()) {
      return possibleItem;
    }

    if (propertyName.startsWith(KsqlConfig.KSQL_FUNCTIONS_PROPERTY_PREFIX)) {
      // Functions properties are free form, so can not be resolved / validated:
      return Optional.of(ConfigItem.unresolved(propertyName));
    }

    return Optional.empty();
  }

  private static Optional<ConfigItem> resolveRequestConfig(final String propertyName) {
    final Optional<ConfigItem> possibleItem = resolveConfig("", REQUEST_CONFIG_DEF, propertyName);
    if (possibleItem.isPresent()) {
      return possibleItem;
    }

    return Optional.empty();
  }

  private static Optional<ConfigItem> resolveConfig(
      final String prefix,
      final ConfigDef def,
      final String propertyName) {

    if (!propertyName.startsWith(prefix)) {
      return Optional.empty();
    }

    final String keyNoPrefix = stripPrefix(propertyName, prefix);
    final ConfigKey configKey = def.configKeys().get(keyNoPrefix);
    if (configKey != null) {
      return Optional.of(ConfigItem.resolved(configKey));
    }

    if (isInternalStreamsConfig(keyNoPrefix)) {
      return Optional.of(ConfigItem.unresolved(keyNoPrefix));
    }

    return Optional.empty();
  }

  private static boolean isInternalStreamsConfig(final String key) {
    // add more internal configs on demand
    return key.equals(InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_OUTER_JOIN_SPURIOUS_RESULTS_FIX)
        || key.equals(InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION);
  }

  private static String stripPrefix(final String maybePrefixedKey, final String prefix) {
    return maybePrefixedKey.startsWith(prefix) ? maybePrefixedKey.substring(prefix.length())
        : maybePrefixedKey;
  }

  static ConfigDef getConfigDef(final Class<? extends AbstractConfig> defClass) {
    try {
      final java.lang.reflect.Field field = defClass.getDeclaredField("CONFIG");
      field.setAccessible(true);
      return (ConfigDef) field.get(null);
    } catch (final Exception exception) {
      throw new IllegalStateException("Failed to initialize config def for " + defClass);
    }
  }

  private static final class PrefixedConfig {

    final String prefix;
    final ConfigDef def;

    private PrefixedConfig(final String prefix, final ConfigDef configDef) {
      this.prefix = Objects.requireNonNull(prefix, "prefix");
      this.def = Objects.requireNonNull(configDef, "configDef");
    }
  }
}
