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
import org.apache.kafka.common.config.ConfigDef.Type;

public class ConfigItem {

  private final ConfigDef def;
  private final ConfigKey key;

  public ConfigItem(final ConfigDef def, final String propertyName) {
    this.def = Objects.requireNonNull(def, "def");
    this.key = def.configKeys().get(Objects.requireNonNull(propertyName, "propertyName"));
  }

  public ConfigDef getDef() {
    return def;
  }

  public String getPropertyName() {
    return key.name;
  }

  public Type getType() {
    return key.type;
  }

  public Object parseValue(final Object value) {
    final Object parsed = ConfigDef.parseType(key.name, value, key.type);

    if (key.validator != null) {
      key.validator.ensureValid(key.name, parsed);
    }

    return parsed;
  }
}