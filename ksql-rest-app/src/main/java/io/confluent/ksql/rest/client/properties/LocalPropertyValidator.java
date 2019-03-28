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

package io.confluent.ksql.rest.client.properties;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.config.PropertyValidator;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

/**
 * This class adds additional validation of properties on top of that provided by the
 * {@code ConfigDef} instances.
 */
class LocalPropertyValidator implements PropertyValidator {

  // Only these config properties can be configured using SET/UNSET commands.
  // Package private for testing.
  static final Set<String> CONFIG_PROPERTY_WHITELIST = ImmutableSet.<String>builder()
      .add(KsqlConfig.SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY)
      .add(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG)
      .add(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
      .add(KsqlConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT)
      .build();

  private static final Map<String, Consumer<Object>> HANDLERS =
      ImmutableMap.<String, Consumer<Object>>builder()
      .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          LocalPropertyValidator::validateConsumerOffsetResetConfig)
      .build();

  LocalPropertyValidator() {
  }

  @Override
  public void validate(final String name, final Object value) {
    if (!CONFIG_PROPERTY_WHITELIST.contains(name)) {
      throw new IllegalArgumentException(String.format("Cannot override property '%s'", name));
    }

    final Consumer<Object> validator = HANDLERS.get(name);
    if (validator != null) {
      validator.accept(value);
    }
  }

  private static void validateConsumerOffsetResetConfig(final Object value) {
    if (value instanceof String && "none".equalsIgnoreCase((String)value)) {
      throw new IllegalArgumentException("'none' is not valid for this property within KSQL");
    }
  }
}
