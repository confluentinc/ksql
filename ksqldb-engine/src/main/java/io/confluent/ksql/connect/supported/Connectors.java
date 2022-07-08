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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.connect.Connector;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;

public enum Connectors implements SupportedConnector {

  JDBC(JdbcSource.JDBC_SOURCE_CLASS, new JdbcSource())
  ;

  public static final String CONNECTOR_CLASS = "connector.class";

  private static final Map<String, SupportedConnector> CONNECTORS = ImmutableMap.copyOf(
      EnumSet.allOf(Connectors.class)
          .stream()
          .collect(Collectors.toMap(
              Connectors::getConnectorClass,
              Function.identity()))
  );

  private final String connectorClass;
  private final SupportedConnector supportedConnector;

  Connectors(final String connectorClass, final SupportedConnector supportedConnector) {
    this.connectorClass = Objects.requireNonNull(connectorClass, "connectorClass");
    this.supportedConnector = Objects.requireNonNull(supportedConnector, "supportedConnector");
  }

  public static Optional<Connector> from(final ConnectorInfo info) {
    final SupportedConnector connector =
        CONNECTORS.get(info.config().get(CONNECTOR_CLASS));
    return connector == null ? Optional.empty() : connector.fromConnectInfo(info);
  }

  public static Map<String, String> resolve(final Map<String, String> configs) {
    final SupportedConnector connector = CONNECTORS.get(configs.get(CONNECTOR_CLASS));
    return new HashMap<>(
        connector == null ? configs : connector.resolveConfigs(configs)
    );
  }

  @Override
  public Optional<Connector> fromConnectInfo(final ConnectorInfo info) {
    return supportedConnector.fromConnectInfo(info);
  }

  @Override
  public Map<String, String> resolveConfigs(final Map<String, String> configs) {
    return supportedConnector.resolveConfigs(configs);
  }

  public String getConnectorClass() {
    return connectorClass;
  }
}
