/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.config;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.util.KsqlConfig;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Resolves Ksql and streams property name to a ConfigItem.
 */
public class KsqlConfigResolver implements ConfigResolver {

  private static final ConfigDef STEAMS_CONFIG_DEF = StreamsConfig.configDef();
  private static final ConfigDef CONSUMER_CONFIG_DEF = getConfigDef(ConsumerConfig.class);
  private static final ConfigDef PRODUCER_CONFIG_DEF = getConfigDef(ProducerConfig.class);
  private static final ConfigDef KSQL_CONFIG_DEF = KsqlConfig.CURRENT_DEF;
  private static final Method PARSE_METHOD = getParseMethod();

  private static final List<PrefixedConfig> STREAM_CONFIG_DEFS = ImmutableList.of(
      new PrefixedConfig(StreamsConfig.CONSUMER_PREFIX, CONSUMER_CONFIG_DEF),
      new PrefixedConfig(StreamsConfig.PRODUCER_PREFIX, PRODUCER_CONFIG_DEF),
      new PrefixedConfig("", CONSUMER_CONFIG_DEF),
      new PrefixedConfig("", PRODUCER_CONFIG_DEF),
      new PrefixedConfig("", STEAMS_CONFIG_DEF)
  );

  @Override
  public  Optional<ConfigItem> resolve(final String propertyName) {
    if (propertyName.startsWith(KsqlConfig.KSQL_STREAMS_PREFIX)
        || !propertyName.startsWith(KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX)) {
      return resolveStreamsConfig(propertyName);
    }

    return resolveKsqlConfig(propertyName);
  }

  private static Optional<ConfigItem> resolveStreamsConfig(final String propertyName) {
    final String key = stripPrefix(propertyName, KsqlConfig.KSQL_STREAMS_PREFIX);

    return STREAM_CONFIG_DEFS
        .stream()
        .map(def -> resolveConfig(def.prefix, def.def, key))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();
  }

  private static Optional<ConfigItem> resolveKsqlConfig(final String propertyName) {
    return resolveConfig("", KSQL_CONFIG_DEF, propertyName);
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
    if (configKey == null) {
      return Optional.empty();
    }

    return Optional.of(new ConfigItem(def, keyNoPrefix));
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

  private Object parseValue(final ConfigDef def, final ConfigKey key, final Object value) {
    try {
      return PARSE_METHOD.invoke(def, key, value, true);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Failed to invoke parseValue", e);
    } catch (InvocationTargetException e) {
      if (e.getTargetException() instanceof RuntimeException) {
        throw (RuntimeException) e.getTargetException();
      }

      throw new RuntimeException(e.getTargetException());
    }
  }

  private static Method getParseMethod() {
    try {
      final Method parseValue = ConfigDef.class.getDeclaredMethod("parseValue",
          ConfigKey.class, Object.class, boolean.class);
      parseValue.setAccessible(true);
      return parseValue;
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("Failed to get parseValue method of Config", e);
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
