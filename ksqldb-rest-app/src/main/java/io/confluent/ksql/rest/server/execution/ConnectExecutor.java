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

package io.confluent.ksql.rest.server.execution;

import com.google.common.collect.Maps;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.connect.supported.Connectors;
import io.confluent.ksql.parser.tree.CreateConnector;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.CreateConnectorEntity;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.WarningEntity;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;

public final class ConnectExecutor {

  private ConnectExecutor() {
  }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<CreateConnector> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final CreateConnector createConnector = statement.getStatement();
    final ConnectClient client = serviceContext.getConnectClient();

    final Optional<KsqlEntity> connectorsResponse = handleIfNotExists(
        statement, createConnector, client);
    if (connectorsResponse.isPresent()) {
      return connectorsResponse;
    }

    final ConnectResponse<ConnectorInfo> response = client.create(
        createConnector.getName(),
        Connectors.resolve(
            Maps.transformValues(
                createConnector.getConfig(),
                l -> l != null ? l.getValue().toString() : null)));

    if (response.datum().isPresent()) {
      return Optional.of(
          new CreateConnectorEntity(
              statement.getMaskedStatementText(),
              response.datum().get()
          )
      );
    }

    if (createConnector.ifNotExists()) {
      final Optional<KsqlEntity> connectors = handleIfNotExists(statement, createConnector, client);

      if (connectors.isPresent()) {
        return connectors;
      }
    }

    return response.error()
        .map(err -> new ErrorEntity(statement.getMaskedStatementText(), err));
  }

  private static Optional<KsqlEntity> handleIfNotExists(
      final ConfiguredStatement<CreateConnector> statement,
      final CreateConnector createConnector,
      final ConnectClient client) {
    if (createConnector.ifNotExists()) {
      final ConnectResponse<List<String>> connectorsResponse = client.connectors();
      if (connectorsResponse.error().isPresent()) {
        return connectorsResponse.error()
            .map(err -> new ErrorEntity(statement.getMaskedStatementText(), err));
      }

      if (checkIfConnectorExists(createConnector, connectorsResponse)) {
        return Optional.of(new WarningEntity(statement.getMaskedStatementText(),
            String.format("Connector %s already exists", createConnector.getName())));
      }
    }
    return Optional.empty();
  }

  private static boolean checkIfConnectorExists(
      final CreateConnector createConnector,
      final ConnectResponse<List<String>> connectorsResponse
  ) {
    return connectorsResponse.datum()
        .get()
        .stream()
        .filter(connector -> StringUtils.equalsIgnoreCase(createConnector.getName(), connector))
        .findAny()
        .isPresent();
  }
}
