/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.connect;

import com.google.common.collect.ImmutableMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.ValueToKey;

/**
 * Templates that help simplify some of the connector configuration for what we expect
 * to be common configurations for KSQL-compatible connectors.
 */
public enum ConnectTemplate implements Function<Map<String, String>, Map<String, String>> {

  JDBC_SOURCE(Connectors.JDBC_SOURCE_CLASS) {
    @Override
    public Map<String, String> apply(final Map<String, String> originals) {
      final Map<String, String> configs = new HashMap<>(originals);
      final String key = configs.remove("key");
      if (key != null) {
        configs.merge(
            "transforms",
            "ksqlCreateKey,ksqlExtractString",
            (a, b) -> String.join(",", a, b));

        configs.put("transforms.ksqlCreateKey.type", ValueToKey.class.getName());
        configs.put("transforms.ksqlCreateKey.fields", key);

        configs.put("transforms.ksqlExtractString.type", ExtractField.Key.class.getName());
        configs.put("transforms.ksqlExtractString.field", key);
      }

      configs.putIfAbsent("key.converter", StringConverter.class.getName());
      configs.putIfAbsent("tasks.max", "1");
      return configs;
    }
  };

  public static Map<String, String> resolve(final Map<String, String> config) {
    final ConnectTemplate template = CLASS_TO_TEMPLATE.get(config.get(Connectors.CONNECTOR_CLASS));
    if (template != null) {
      return template.apply(config);
    }

    return config;
  }

  private static final Map<String, ConnectTemplate> CLASS_TO_TEMPLATE = ImmutableMap.copyOf(
      EnumSet.allOf(ConnectTemplate.class)
          .stream()
          .collect(Collectors.toMap(
              ConnectTemplate::getConnectorClass,
              Function.identity()))
  );

  private final String connectorClass;

  ConnectTemplate(final String connectorClass) {
    this.connectorClass = Objects.requireNonNull(connectorClass, "connectorClass");
  }

  private String getConnectorClass() {
    return connectorClass;
  }
}
