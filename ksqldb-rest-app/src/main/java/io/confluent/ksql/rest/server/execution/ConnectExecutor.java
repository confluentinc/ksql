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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.connect.supported.Connectors;
import io.confluent.ksql.parser.tree.CreateConnector;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ConfigInfos;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import io.confluent.ksql.rest.entity.ConnectorType;
import io.confluent.ksql.rest.entity.CreateConnectorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.WarningEntity;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.HttpStatus;

public final class ConnectExecutor {

  private static final ConnectorInfo DUMMY_CREATE_RESPONSE =
      new ConnectorInfo("dummy", ImmutableMap.of(), ImmutableList.of(), ConnectorType.UNKNOWN);

  private ConnectExecutor() {
  }

  public static StatementExecutorResponse execute(
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
      return StatementExecutorResponse.handled(connectorsResponse);
    }

    final ConnectResponse<ConnectorInfo> response = client.create(
        createConnector.getName(),
        buildConnectorConfig(createConnector));

    if (response.datum().isPresent()) {
      return StatementExecutorResponse.handled(Optional.of(
          new CreateConnectorEntity(
              statement.getMaskedStatementText(),
              response.datum().get()
          )
      ));
    }

    if (response.error().isPresent()) {
      final String errorMsg = "Failed to create connector: " + response.error().get();
      throw new KsqlRestException(EndpointResponse.create()
          .status(response.httpCode())
          .entity(new KsqlErrorMessage(Errors.toErrorCode(response.httpCode()), errorMsg))
          .build()
      );
    }

    throw new IllegalStateException("Either response.datum() or response.error() must be present");
  }

  public static StatementExecutorResponse validate(
      final ConfiguredStatement<CreateConnector> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final CreateConnector createConnector = statement.getStatement();
    final ConnectClient client = serviceContext.getConnectClient();

    if (checkForExistingConnector(statement, createConnector, client)) {
      final String errorMsg = String.format(
          "Connector %s already exists", createConnector.getName());
      throw new KsqlRestException(EndpointResponse.create()
          .status(HttpStatus.SC_CONFLICT)
          .entity(new KsqlErrorMessage(Errors.toErrorCode(HttpStatus.SC_CONFLICT), errorMsg))
          .build()
      );
    }

    final List<String> errors = validateConfigs(createConnector, client);
    if (!errors.isEmpty()) {
      final String errorMessage = "Validation error: " + String.join("\n", errors);
      throw new KsqlException(errorMessage);
    }

    return StatementExecutorResponse.handled(Optional.of(new CreateConnectorEntity(
        statement.getMaskedStatementText(),
        DUMMY_CREATE_RESPONSE
    )));
  }

  private static List<String> validateConfigs(
      final CreateConnector createConnector, final ConnectClient client) {
    final Map<String, String> config = buildConnectorConfig(createConnector);

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

  private static Map<String, String> buildConnectorConfig(final CreateConnector createConnector) {
    final Map<String, String> config = Connectors.resolve(
        Maps.transformValues(
            createConnector.getConfig(),
            l -> l != null ? l.getValue().toString() : null));
    config.put("name", createConnector.getName());
    return config;
  }

  /**
   * @return true if there already exists a connector with this name when none is expected.
   *         This scenario is checked for as part of validation in order to fail fast, since
   *         otherwise execution would fail with this same error.
   */
  private static boolean checkForExistingConnector(
      final ConfiguredStatement<CreateConnector> statement,
      final CreateConnector createConnector,
      final ConnectClient client
  ) {
    if (createConnector.ifNotExists()) {
      // nothing to check since the statement is not meant to fail even if the
      // connector already exists
      return false;
    }

    final ConnectResponse<List<String>> connectorsResponse = client.connectors();
    if (connectorsResponse.error().isPresent()) {
      throw new KsqlServerException("Failed to check for existing connector: "
          + connectorsResponse.error().get());
    }
    return connectorExists(createConnector, connectorsResponse);
  }

  private static Optional<KsqlEntity> handleIfNotExists(
      final ConfiguredStatement<CreateConnector> statement,
      final CreateConnector createConnector,
      final ConnectClient client) {
    if (createConnector.ifNotExists()) {
      final ConnectResponse<List<String>> connectorsResponse = client.connectors();
      if (connectorsResponse.error().isPresent()) {
        throw new KsqlServerException("Failed to check for existing connector: "
            + connectorsResponse.error().get());
      }

      if (connectorExists(createConnector, connectorsResponse)) {
        return Optional.of(new WarningEntity(statement.getMaskedStatementText(),
            String.format("Connector %s already exists", createConnector.getName())));
      }
    }
    return Optional.empty();
  }

  private static boolean connectorExists(
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
