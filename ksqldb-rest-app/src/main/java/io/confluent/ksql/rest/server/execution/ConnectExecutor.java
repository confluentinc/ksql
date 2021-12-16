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

import com.google.common.collect.ImmutableList;
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
import io.confluent.ksql.util.ParserUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;

public final class ConnectExecutor {
  private final ConnectServerErrors connectErrorHandler;

  ConnectExecutor(final ConnectServerErrors connectErrorHandler) {
    this.connectErrorHandler = connectErrorHandler;
  }

  public StatementExecutorResponse execute(
      final ConfiguredStatement<CreateConnector> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final CreateConnector createConnector = statement.getStatement();
    final ConnectClient client = serviceContext.getConnectClient();

    final List<String> errors = validate(createConnector, client);
    if (!errors.isEmpty()) {
      final String errorMessage = "Validation error: " + String.join("\n", errors);
      return StatementExecutorResponse.handled(Optional.of(new ErrorEntity(
          statement.getStatementText(), errorMessage)));
    }

    final Optional<KsqlEntity> connectorsResponse = handleIfNotExists(
        statement, createConnector, client);
    if (connectorsResponse.isPresent()) {
      return StatementExecutorResponse.handled(connectorsResponse);
    }

    final ConnectResponse<ConnectorInfo> response = client.create(
        createConnector.getName(),
        Connectors.resolve(
            Maps.transformValues(
                createConnector.getConfig(),
                l -> l != null ? l.getValue().toString() : null)));

    if (response.datum().isPresent()) {
      return StatementExecutorResponse.handled(Optional.of(
          new CreateConnectorEntity(
              statement.getStatementText(),
              response.datum().get()
          )
      ));
    }

    if (createConnector.ifNotExists()) {
      final Optional<KsqlEntity> connectors = handleIfNotExists(statement, createConnector, client);

      if (connectors.isPresent()) {
        return StatementExecutorResponse.handled(connectors);
      }
    }

    return StatementExecutorResponse.handled(connectErrorHandler.handle(statement, response));
  }

  private static List<String> validate(final CreateConnector createConnector,
      final ConnectClient client) {
    final Map<String, String> config = new HashMap<>(createConnector.getConfig().size());
    createConnector.getConfig().forEach((k, v) ->
        // Parsing the statement wraps string fields with single quotes which breaks the
        // validation.
        config.put(k, ParserUtil.unquote(v.toString(),"'")));
    config.put("name", createConnector.getName());

    final String connectorType = config.get("connector.class");
    // Request with an empty connector type in the url results in 404.
    // Request with a connector type that contains only spaces results in 405.
    // In both cases it is user-friendlier to return the actual error.
    if (connectorType == null) {
      // Return error message identical to the one produced by the connector
      return ImmutableList.of(String.format(
          "Connector config %s contains no connector type", config));
    } else if (connectorType.trim().isEmpty()) {
      return ImmutableList.of("Connector type cannot be empty");
    }

    final ConnectResponse<ConfigInfos> response = client.validate(connectorType, config);
    if (response.error().isPresent()) {
      return ImmutableList.of(response.error().get());
    } else if (response.datum().isPresent()) {
      return response
          .datum()
          .get()
          .values()
          .stream()
          .filter(configInfo -> !configInfo.configValue().errors().isEmpty())
          .map(configInfo -> configInfo.configValue().name()
              + " - "
              + String.join(". ", configInfo.configValue().errors()))
          .collect(Collectors.toList());
    }
    return ImmutableList.of();
  }

  private static Optional<KsqlEntity> handleIfNotExists(
      final ConfiguredStatement<CreateConnector> statement,
      final CreateConnector createConnector,
      final ConnectClient client) {
    if (createConnector.ifNotExists()) {
      final ConnectResponse<List<String>> connectorsResponse = client.connectors();
      if (connectorsResponse.error().isPresent()) {
        return connectorsResponse.error()
            .map(err -> new ErrorEntity(statement.getStatementText(), err));
      }

      if (checkIfConnectorExists(createConnector, connectorsResponse)) {
        return Optional.of(new WarningEntity(statement.getStatementText(),
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
