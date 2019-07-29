/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.configdef;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

/**
 * Custom {@link org.apache.kafka.common.config.ConfigDef.Validator}s
 */
public final class ConfigValidators {

  private ConfigValidators() {
  }

  public static <T extends Enum<T>> Validator enumValues(final Class<T> enumClass) {
    final String[] enumValues = EnumSet.allOf(enumClass)
        .stream()
        .map(Object::toString)
        .toArray(String[]::new);

    final String[] validValues = Arrays.copyOf(enumValues, enumValues.length + 1);
    validValues[enumValues.length] = null;

    return ValidCaseInsensitiveString.in(validValues);
  }

  public static final class ValidCaseInsensitiveString implements Validator {

    private final List<String> validStrings;

    private ValidCaseInsensitiveString(final String... validStrings) {
      this.validStrings = Arrays.stream(requireNonNull(validStrings, "validStrings"))
          .map(v -> v == null ? null : v.toUpperCase())
          .collect(Collectors.toList());
    }

    public static ValidCaseInsensitiveString in(final String... validStrings) {
      return new ValidCaseInsensitiveString(validStrings);
    }

    @Override
    public void ensureValid(final String name, final Object value) {
      final String s = (String) value;
      if (!validStrings.contains(s == null ? null : s.toUpperCase())) {
        throw new ConfigException(name, value, "String must be one of: "
            + String.join(", ", validStrings));
      }
    }

    public String toString() {
      return "[" + String.join(", ", validStrings) + "]";
    }
  }
}
