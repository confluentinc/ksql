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

package io.confluent.ksql.rest.client.properties;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
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

@SuppressWarnings("OptionalAssignedToNull")
@SuppressFBWarnings(value = "NP_OPTIONAL_RETURN_NULL", justification = "Tri-state use")
class LocalPropertyParser implements PropertyParser {

  private static final ConfigDef STEAMS_CONFIG_DEF = StreamsConfig.configDef();
  private static final ConfigDef CONSUMER_CONFIG_DEF = getConfigDef(ConsumerConfig.class);
  private static final ConfigDef PRODUCER_CONFIG_DEF = getConfigDef(ProducerConfig.class);
  private static final Method PARSE_METHOD = getParseMethod();
  private static final List<Handler> HANDLERS =
      ImmutableList.<Handler>builder()
          .add(LocalPropertyParser::handleStreamConfig)
          .add(LocalPropertyParser::handleConsumerConfig)
          .add(LocalPropertyParser::handleProducerConfig)
          .add(LocalPropertyParser::handleKsqlConstants)
          .add(LocalPropertyParser::handleKsqlConfig)
          .build();

  private final PropertiesValidator validator;

  LocalPropertyParser() {
    this(new LocalPropertiesValidator());
  }

  LocalPropertyParser(final PropertiesValidator validator) {
    this.validator = Objects.requireNonNull(validator, "validator");
  }

  @Override
  public Object parse(final String property, final Object value) {
    return HANDLERS.stream()
        .map(handler -> handler.maybeHandle(this, property, value))
        .filter(Objects::nonNull)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException(String.format(
            "Not recognizable as ksql, streams, consumer, or producer property: '%s'", property)))
        .orElse(null);
  }

  private Optional<Object> handleStreamConfig(final String property, final Object value) {
    if (STEAMS_CONFIG_DEF.configKeys().containsKey(property)) {
      return parseValue(StreamsConfig.configDef(), property, value);
    }

    if (property.startsWith(KsqlConfig.KSQL_STREAMS_PREFIX)) {
      final String nonPrefixed = validatePrefixedProperty(
          property, KsqlConfig.KSQL_STREAMS_PREFIX, STEAMS_CONFIG_DEF, "streams");

      return parseValue(STEAMS_CONFIG_DEF, nonPrefixed, value);
    }

    return null;
  }

  private Optional<Object> handleConsumerConfig(final String property, final Object value) {
    if (CONSUMER_CONFIG_DEF.configKeys().containsKey(property)) {
      return parseValue(CONSUMER_CONFIG_DEF, property, value);
    }

    if (property.startsWith(StreamsConfig.CONSUMER_PREFIX)) {
      final String nonPrefixed = validatePrefixedProperty(
          property, StreamsConfig.CONSUMER_PREFIX, CONSUMER_CONFIG_DEF, "consumer");

      return parseValue(CONSUMER_CONFIG_DEF, nonPrefixed, value);
    }

    return null;
  }

  private Optional<Object> handleProducerConfig(final String property, final Object value) {
    if (PRODUCER_CONFIG_DEF.configKeys().containsKey(property)) {
      return parseValue(PRODUCER_CONFIG_DEF, property, value);
    }

    if (property.startsWith(StreamsConfig.PRODUCER_PREFIX)) {
      final String nonPrefixed = validatePrefixedProperty(
          property, StreamsConfig.PRODUCER_PREFIX, PRODUCER_CONFIG_DEF, "producer");

      return parseValue(PRODUCER_CONFIG_DEF, nonPrefixed, value);
    }

    return null;
  }

  private Optional<Object> handleKsqlConstants(final String property, final Object value) {
    if (property.equalsIgnoreCase(DdlConfig.AVRO_SCHEMA)
        || property.equalsIgnoreCase(KsqlConstants.RUN_SCRIPT_STATEMENTS_CONTENT)) {

      validator.validate(property, value);
      return Optional.ofNullable(value);
    }

    return null;
  }

  private Optional<Object> handleKsqlConfig(final String property, final Object value) {
    if (property.startsWith(KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX)) {
      return parseValue(KsqlConfig.CURRENT_DEF, property, value);
    }

    return null;
  }

  private Optional<Object> parseValue(final ConfigDef def, final String name, final Object value) {
    final ConfigKey key = def.configKeys().get(name);
    final Object parsedValue = parseValue(def, key, value);

    validator.validate(name, parsedValue);
    return Optional.ofNullable(parsedValue);
  }

  private Object parseValue(final ConfigDef def, final ConfigKey key, final Object value) {
    try {
      return PARSE_METHOD.invoke(def, key, value, true);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Failed to invoke parseValue", e);
    } catch (InvocationTargetException e) {
      if (e.getTargetException() instanceof RuntimeException) {
        throw (RuntimeException)e.getTargetException();
      }

      throw new RuntimeException(e.getTargetException());
    }
  }

  private static String validatePrefixedProperty(
      final String property,
      final String prefix,
      final ConfigDef configDef,
      final String propType) {

    final String nonPrefixed = property.substring(prefix.length());
    final ConfigKey configKey = configDef.configKeys().get(nonPrefixed);
    if (configKey == null) {
      throw new IllegalArgumentException(
          String.format("Invalid %s property: '%s'", propType, nonPrefixed));
    }
    return nonPrefixed;
  }

  private static ConfigDef getConfigDef(final Class<? extends AbstractConfig> defClass) {
    try {
      final java.lang.reflect.Field field = defClass.getDeclaredField("CONFIG");
      field.setAccessible(true);
      return (ConfigDef) field.get(null);
    } catch (final Exception exception) {
      throw new IllegalStateException("Failed to initialize config def for " + defClass);
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

  private interface Handler {

    Optional<Object> maybeHandle(LocalPropertyParser parser, String name, Object value);
  }
}
