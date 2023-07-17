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

import io.confluent.ksql.util.KsqlConfig;
import java.net.URL;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

/**
 * Custom {@link org.apache.kafka.common.config.ConfigDef.Validator}s
 */
public final class ConfigValidators {

  private ConfigValidators() {
  }

  /**
   * Validator that tests the STRING property can be parsed by the supplied {@code parser}.
   * @param parser the parser.
   * @return the validator
   */
  public static Validator parses(final Function<String, ?> parser) {
    return (name, val) -> {
      if (val != null && !(val instanceof String)) {
        throw new IllegalArgumentException("validator should only be used with STRING defs");
      }
      try {
        parser.apply((String)val);
      } catch (final Exception e) {
        throw new ConfigException("Configuration " + name + " is invalid: " + e.getMessage());
      }
    };
  }

  /**
   * Validator that allows null values and calls the {@code delegate} for any non-null values.
   * @param delegate the delegate to call for non-null values.
   * @return the validator.
   */
  public static Validator nullsAllowed(final Validator delegate) {
    return (name, value) -> {
      if (value == null) {
        return;
      }

      delegate.ensureValid(name, value);
    };
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

  public static Validator validUrl() {
    return (name, val) -> {
      if (!(val instanceof String)) {
        throw new IllegalArgumentException("validator should only be used with STRING defs");
      }
      try {
        new URL((String)val);
      } catch (final Exception e) {
        throw new ConfigException(name, val, "Not valid URL: " + e.getMessage());
      }
    };
  }

  public static Validator validRegex() {
    return (name, val) -> {
      if (!(val instanceof List)) {
        throw new IllegalArgumentException("validator should only be used with "
            + "LIST of STRING defs");
      }

      final StringBuilder regexBuilder = new StringBuilder();
      for (Object item : (List)val) {
        if (!(item instanceof String)) {
          throw new IllegalArgumentException("validator should only be used with "
              + "LIST of STRING defs");
        }

        if (regexBuilder.length() > 0) {
          regexBuilder.append("|");
        }

        regexBuilder.append((String)item);
      }

      try {
        Pattern.compile(regexBuilder.toString());
      } catch (final Exception e) {
        throw new ConfigException(name, val, "Not valid regular expression: " + e.getMessage());
      }
    };
  }

  public static Validator zeroOrPositive() {
    return (name, val) -> {
      if (val instanceof Long) {
        if (((Long) val) < 0) {
          throw new ConfigException(name, val, "Not >= 0");
        }
      } else if (val instanceof Integer) {
        if (((Integer) val) < 0) {
          throw new ConfigException(name, val, "Not >= 0");
        }
      } else {
        throw new IllegalArgumentException("validator should only be used with int, long");
      }
    };
  }

  public static Validator oneOrMore() {
    return (name, val) -> {
      if (val instanceof Long) {
        if (((Long) val) < 1) {
          throw new ConfigException(name, val, "Not >= 1");
        }
      } else if (val instanceof Integer) {
        if (((Integer) val) < 1) {
          throw new ConfigException(name, val, "Not >= 1");
        }
      } else {
        throw new IllegalArgumentException("validator should only be used with int, long");
      }
    };
  }

  public static Validator intList() {
    return (name, val) -> {
      if (!(val instanceof List)) {
        throw new ConfigException(name, val, "Must be a list");
      }
      @SuppressWarnings("unchecked")
      final List<String> list = (List<String>) val;
      list.forEach(intStr -> {
        try {
          Integer.parseInt(intStr);
        } catch (NumberFormatException e) {
          throw new ConfigException(name, intStr, "Not an integer");
        }
      });
    };
  }

  public static Validator mapWithIntKeyDoubleValue() {
    return (name, val) -> {
      if (!(val instanceof String)) {
        throw new ConfigException(name, val, "Must be a string");
      }

      final String str = (String) val;
      final Map<String, String> map = KsqlConfig.parseStringAsMap(name, str);
      map.forEach((keyStr, valueStr) -> {
        try {
          Integer.parseInt(keyStr);
        } catch (NumberFormatException e) {
          throw new ConfigException(name, keyStr, "Not an int");
        }
        try {
          Double.parseDouble(valueStr);
        } catch (NumberFormatException e) {
          throw new ConfigException(name, valueStr, "Not a double");
        }
      });
    };
  }

  public static Validator mapWithDoubleValue() {
    return (name, val) -> {
      if (!(val instanceof String)) {
        throw new ConfigException(name, val, "Must be a string");
      }

      final String str = (String) val;
      final Map<String, String> map = KsqlConfig.parseStringAsMap(name, str);
      map.forEach((k, valueStr) -> {
        try {
          Double.parseDouble(valueStr);
        } catch (NumberFormatException e) {
          throw new ConfigException(name, valueStr, "Not a double");
        }
      });
    };
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
