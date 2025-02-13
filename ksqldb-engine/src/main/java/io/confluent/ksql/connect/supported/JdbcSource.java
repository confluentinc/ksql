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

package io.confluent.ksql.connect.supported;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import io.confluent.ksql.connect.Connector;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.ValueToKey;

public final class JdbcSource implements SupportedConnector {

  static final String JDBC_SOURCE_CLASS = "io.confluent.connect.jdbc.JdbcSourceConnector";

  @Override
  public Optional<Connector> fromConnectInfo(final ConnectorInfo info) {
    final Map<String, String> properties = info.config();
    return fromConfigs(properties);
  }

  @VisibleForTesting
  Optional<Connector> fromConfigs(final Map<String, String> properties) {
    final String name = properties.get("name");
    return Optional.of(new Connector(
        name,
        DataSourceType.KTABLE,
        extractKeyNameFromSmt(properties).orElse(null)
    ));
  }

  @Override
  public Map<String, String> resolveConfigs(final Map<String, String> configs) {
    final Map<String, String> resolved = new HashMap<>(configs);
    final String key = resolved.remove("key");
    if (key != null) {
      resolved.merge(
          "transforms",
          "ksqlCreateKey,ksqlExtractString",
          (a, b) -> String.join(",", a, b));

      resolved.put("transforms.ksqlCreateKey.type", ValueToKey.class.getName());
      resolved.put("transforms.ksqlCreateKey.fields", key);

      resolved.put("transforms.ksqlExtractString.type", ExtractField.Key.class.getName());
      resolved.put("transforms.ksqlExtractString.field", key);
    }

    resolved.putIfAbsent("tasks.max", "1");
    return resolved;
  }

  /**
   * JDBC connector does not necessarily have an easy way to define the key field (or the primary
   * column in the database). Most configurations of JDBC, therefore, will specify an extract field
   * transform to determine the key of the table - the built in one in connect is ExtractField$Key,
   * though it is not necessary that the connect is configured with this Single Message Transform.
   * If it is not configured such, we will not be able to determine the key and the user will need
   * to manually import the connector.
   *
   * <p>This is pretty hacky, and we need to figure out a better long-term way to determine the key
   * column from a connector.</p>
   */
  private static Optional<String> extractKeyNameFromSmt(final Map<String, String> properties) {
    final String transformsString = properties.get("transforms");
    if (transformsString == null) {
      return Optional.empty();
    }

    final List<String> transforms = Splitter.on(',').splitToList(transformsString);
    for (final String transform : transforms) {
      final String transformType = properties.get("transforms." + transform + ".type");
      if (transformType != null && transformType.contains("ExtractField$Key")) {
        return Optional.ofNullable(properties.get("transforms." + transform + ".field"));
      }
    }

    return Optional.empty();
  }

}
