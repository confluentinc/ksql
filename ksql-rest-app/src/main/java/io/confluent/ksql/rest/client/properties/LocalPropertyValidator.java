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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.config.PropertyValidator;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

class LocalPropertyValidator implements PropertyValidator {

  private static final Map<String, Consumer<Object>> HANDLERS =
      ImmutableMap.<String, Consumer<Object>>builder()
      .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          LocalPropertyValidator::validateConsumerOffsetResetConfig)
      .build();

  private final Set<String> immutableProps;

  LocalPropertyValidator() {
    this(KsqlEngine.getImmutableProperties());
  }

  LocalPropertyValidator(final Collection<String> immutableProps) {
    this.immutableProps = ImmutableSet.copyOf(
        Objects.requireNonNull(immutableProps, "immutableProps"));
  }

  @Override
  public void validate(final String name, final Object value) {
    if (immutableProps.contains(name)) {
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
