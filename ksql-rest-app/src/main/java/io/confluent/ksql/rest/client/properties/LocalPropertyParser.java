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

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.streams.StreamsConfig;

class LocalPropertyParser implements PropertyParser {

  private static final ConfigDef CONSUMER_CONFIG_DEF = getConfigDef(ConsumerConfig.class);
  private static final ConfigDef PRODUCER_CONFIG_DEF = getConfigDef(ProducerConfig.class);
  private static final Method PARSE_METHOD = getParseMethod();

  private final PropertiesValidator validator;

  LocalPropertyParser() {
    this(new LocalPropertiesValidator());
  }

  LocalPropertyParser(final PropertiesValidator validator) {
    this.validator = Objects.requireNonNull(validator, "validator");
  }

  @Override
  public Object parse(final String property, final Object value) {
    final String parsedProperty;
    final ConfigDef def;

    if (StreamsConfig.configDef().configKeys().containsKey(property)) {
      def = StreamsConfig.configDef();
      parsedProperty = property;
    } else if (CONSUMER_CONFIG_DEF.configKeys().containsKey(property)) {
      def = CONSUMER_CONFIG_DEF;
      parsedProperty = property;
    } else if (PRODUCER_CONFIG_DEF.configKeys().containsKey(property)) {
      def = PRODUCER_CONFIG_DEF;
      parsedProperty = property;
    } else if (property.startsWith(StreamsConfig.CONSUMER_PREFIX)) {
      parsedProperty = property.substring(StreamsConfig.CONSUMER_PREFIX.length());
      ensureConsumerProperty(parsedProperty);
      def = CONSUMER_CONFIG_DEF;
    } else if (property.startsWith(StreamsConfig.PRODUCER_PREFIX)) {
      parsedProperty = property.substring(StreamsConfig.PRODUCER_PREFIX.length());
      ensureProducerProperty(parsedProperty);
      def = PRODUCER_CONFIG_DEF;
    } else if (property.startsWith(KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX)) {
      def = KsqlConfig.CURRENT_DEF;
      parsedProperty = property;
    } else if (property.equalsIgnoreCase(DdlConfig.AVRO_SCHEMA)
        || property.equalsIgnoreCase(KsqlConstants.RUN_SCRIPT_STATEMENTS_CONTENT)) {

      validator.validate(property, value);
      return value;
    } else {
      throw new IllegalArgumentException(String.format(
          "Not recognizable as ksql, streams, consumer, or producer property: '%s'", property));
    }

    final ConfigKey key = def.configKeys().get(parsedProperty);
    final Object parsedValue = parseValue(def, key, value);

    validator.validate(parsedProperty, parsedValue);
    return parsedValue;
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

  @SuppressWarnings("ConstantConditions")
  private static void ensureProducerProperty(final String parsedProperty) {
    final ConfigDef.ConfigKey configKey = PRODUCER_CONFIG_DEF.configKeys().get(parsedProperty);
    if (configKey == null) {
      throw new IllegalArgumentException(
          String.format("Invalid producer property: '%s'", parsedProperty));
    }
  }

  @SuppressWarnings("ConstantConditions")
  private static void ensureConsumerProperty(final String parsedProperty) {
    final ConfigDef.ConfigKey configKey = CONSUMER_CONFIG_DEF.configKeys().get(parsedProperty);
    if (configKey == null) {
      throw new IllegalArgumentException(
          String.format("Invalid consumer property: '%s'", parsedProperty));
    }
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
}
