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

import java.util.Objects;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;

public interface ConfigItem {

  /**
   * @return the name of the property.
   */
  String getPropertyName();

  /**
   * Parse and validate the value for this config item.
   *
   * <p>Parsing and validating is done by the underlying {@link ConfigDef}, where known.
   * For unresolved items the {@code value} passes through as-is.
   *
   * @param value the raw item to parse.
   * @return the parsed and validated value.
   */
  Object parseValue(Object value);

  /**
   * Convert the supplied {@code value} to a {@code String}.
   *
   * <p>Passwords are obfuscated.
   * For unresolved items the {@code value} is converted to a {@code String}.
   *
   * @param value the value of the property.
   * @return the obfuscated value.
   */
  String convertToString(Object value);

  static ConfigItem resolved(final ConfigKey key) {
    return new ConfigItem.Resolved(key);
  }

  static ConfigItem unresolved(final String propertyName) {
    return new ConfigItem.Unresolved(propertyName);
  }

  class Unresolved implements ConfigItem {

    private final String propertyName;

    private Unresolved(final String propertyName) {
      this.propertyName = Objects.requireNonNull(propertyName, "propertyName");
    }

    @Override
    public String getPropertyName() {
      return propertyName;
    }

    @Override
    public Object parseValue(final Object value) {
      return value;
    }

    @Override
    public String convertToString(final Object value) {
      return value == null ? "NULL" : value.toString();
    }
  }

  class Resolved implements ConfigItem {

    private final ConfigKey key;

    private Resolved(final ConfigKey key) {
      this.key = Objects.requireNonNull(key, "key");
    }

    @Override
    public String getPropertyName() {
      return key.name;
    }

    @Override
    public Object parseValue(final Object value) {
      final Object parsed = ConfigDef.parseType(key.name, value, key.type);
      if (key.validator != null) {
        key.validator.ensureValid(key.name, parsed);
      }
      return parsed;
    }

    @Override
    public String convertToString(final Object value) {
      final Object parsed = parseValue(value);
      return ConfigDef.convertToString(parsed, key.type);
    }

    ConfigKey getKey() {
      return key;
    }
  }
}
