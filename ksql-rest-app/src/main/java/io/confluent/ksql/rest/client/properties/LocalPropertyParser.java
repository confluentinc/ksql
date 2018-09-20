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
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;

class LocalPropertyParser implements PropertyParser {

  private static final ConfigDef CONSUMER_CONFIG_DEF = getConfigDef(ConsumerConfig.class);
  private static final ConfigDef PRODUCER_CONFIG_DEF = getConfigDef(ProducerConfig.class);

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
    final ConfigDef.Type type;
    if (StreamsConfig.configDef().configKeys().containsKey(property)) {
      type = StreamsConfig.configDef().configKeys().get(property).type;
      parsedProperty = property;
    } else if (CONSUMER_CONFIG_DEF.configKeys().containsKey(property)) {
      type = CONSUMER_CONFIG_DEF.configKeys().get(property).type;
      parsedProperty = property;
    } else if (PRODUCER_CONFIG_DEF.configKeys().containsKey(property)) {
      type = PRODUCER_CONFIG_DEF.configKeys().get(property).type;
      parsedProperty = property;
    } else if (property.startsWith(StreamsConfig.CONSUMER_PREFIX)) {
      parsedProperty = property.substring(StreamsConfig.CONSUMER_PREFIX.length());
      type = parseConsumerProperty(parsedProperty);
    } else if (property.startsWith(StreamsConfig.PRODUCER_PREFIX)) {
      parsedProperty = property.substring(StreamsConfig.PRODUCER_PREFIX.length());
      type = parseProducerProperty(parsedProperty);
    } else if (property.equalsIgnoreCase(DdlConfig.AVRO_SCHEMA)
        || property.equalsIgnoreCase(KsqlConstants.RUN_SCRIPT_STATEMENTS_CONTENT)
        || property.startsWith(KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX)) {
      validator.validate(property, value);
      return value;
    } else {
      throw new IllegalArgumentException(String.format(
          "Not recognizable as ksql, streams, consumer, or producer property: '%s'", property));
    }

    final Object parsedValue = ConfigDef.parseType(parsedProperty, value, type);

    validator.validate(parsedProperty, parsedValue);
    return parsedValue;
  }

  @SuppressWarnings("ConstantConditions")
  private static ConfigDef.Type parseProducerProperty(final String parsedProperty) {
    final ConfigDef.ConfigKey configKey = PRODUCER_CONFIG_DEF.configKeys().get(parsedProperty);
    if (configKey == null) {
      throw new IllegalArgumentException(
          String.format("Invalid producer property: '%s'", parsedProperty));
    }

    return configKey.type;
  }

  @SuppressWarnings("ConstantConditions")
  private static ConfigDef.Type parseConsumerProperty(final String parsedProperty) {
    final ConfigDef.ConfigKey configKey = CONSUMER_CONFIG_DEF.configKeys().get(parsedProperty);
    if (configKey == null) {
      throw new IllegalArgumentException(
          String.format("Invalid consumer property: '%s'", parsedProperty));
    }
    return configKey.type;
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
}
