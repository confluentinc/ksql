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

import com.google.common.base.Splitter;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;

final class Connectors {

  static final String CONNECTOR_CLASS = "connector.class";
  static final String JDBC_SOURCE_CLASS = "io.confluent.connect.jdbc.JdbcSourceConnector";

  private Connectors() {
  }

  static Optional<Connector> fromConnectInfo(final ConnectorInfo connectorInfo) {
    return fromConnectInfo(connectorInfo.config());
  }

  @SuppressWarnings("SwitchStatementWithTooFewBranches") // will soon expand to more
  static Optional<Connector> fromConnectInfo(final Map<String, String> properties) {
    final String clazz = properties.get(CONNECTOR_CLASS);
    if (clazz == null) {
      return Optional.empty();
    }

    switch (clazz) {
      case JDBC_SOURCE_CLASS:
        return Optional.of(jdbc(properties));
      default:
        return Optional.empty();
    }
  }

  private static Connector jdbc(final Map<String, String> properties) {
    final String name = properties.get("name");
    final String prefix = properties.get("topic.prefix");

    return new Connector(
        name,
        topic -> topic.startsWith(prefix),
        topic -> clean(name + "_" + topic.substring(prefix.length())),
        DataSourceType.KTABLE,
        extractKeyNameFromSMT(properties).orElse(null)
    );
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
  private static Optional<String> extractKeyNameFromSMT(final Map<String, String> properties) {
    final String transformsString = properties.get("transforms");
    if (transformsString == null) {
      return Optional.empty();
    }

    final List<String> transforms = Splitter.on(',').splitToList(transformsString);
    for (String transform : transforms) {
      final String transformType = properties.get("transforms." + transform + ".type");
      if (transformType != null && transformType.contains("ExtractField$Key")) {
        return Optional.ofNullable(properties.get("transforms." + transform + ".field"));
      }
    }

    return Optional.empty();
  }

  private static String clean(final String name) {
    return name.replace('-', '_').replace('.', '_');
  }

}
