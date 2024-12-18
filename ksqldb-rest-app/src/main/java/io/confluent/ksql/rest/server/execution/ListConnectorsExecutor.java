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

import static io.confluent.ksql.rest.entity.ConnectorStateInfo.RUNNING;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.ListConnectors;
import io.confluent.ksql.parser.tree.ListConnectors.Scope;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ConfigInfos;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import io.confluent.ksql.rest.entity.ConnectorList;
import io.confluent.ksql.rest.entity.ConnectorStateInfo;
import io.confluent.ksql.rest.entity.ConnectorStateInfo.AbstractState;
import io.confluent.ksql.rest.entity.ConnectorType;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.SimpleConnectorInfo;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class ListConnectorsExecutor {

  private ListConnectorsExecutor() {
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public static StatementExecutorResponse execute(
      final ConfiguredStatement<ListConnectors> configuredStatement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext ksqlExecutionContext,
      final ServiceContext serviceContext
  ) {
    final ConnectClient connectClient = serviceContext.getConnectClient();
    final ConnectResponse<List<String>> connectors = serviceContext.getConnectClient().connectors();
    if (connectors.error().isPresent()) {
      final String errorMsg = "Failed to list connectors: " + connectors.error().get();
      throw new KsqlRestException(EndpointResponse.create()
          .status(connectors.httpCode())
          .entity(new KsqlErrorMessage(Errors.toErrorCode(connectors.httpCode()), errorMsg))
          .build()
      );
    }

    final List<SimpleConnectorInfo> infos = new ArrayList<>();
    final List<KsqlWarning> warnings = new ArrayList<>();
    final Scope scope = configuredStatement.getStatement().getScope();

    for (final String name : connectors.datum().get()) {
      final ConnectResponse<ConnectorInfo> response = connectClient.describe(name);

      if (response.datum().filter(i -> inScope(i.type(), scope)).isPresent()) {
        final ConnectResponse<ConnectorStateInfo> status = connectClient.status(name);
        infos.add(fromConnectorInfoResponse(name, response, status));
      } else if (response.error().isPresent()) {
        if (scope == Scope.ALL) {
          infos.add(new SimpleConnectorInfo(name, ConnectorType.UNKNOWN, null, null));
        }

        warnings.add(
            new KsqlWarning(
                String.format(
                    "Could not describe connector %s: %s",
                    name,
                    response.error().get())));
      }
    }

    return StatementExecutorResponse.handled(Optional.of(
        new ConnectorList(
            configuredStatement.getMaskedStatementText(),
            warnings,
            infos)
    ));
  }

  private static boolean inScope(final ConnectorType type, final Scope scope) {
    switch (scope) {
      case SOURCE:  return type == ConnectorType.SOURCE;
      case SINK:    return type == ConnectorType.SINK;
      case ALL:     return true;
      default:      throw new IllegalArgumentException("Unexpected scope: " + scope);
    }
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private static SimpleConnectorInfo fromConnectorInfoResponse(
      final String name,
      final ConnectResponse<ConnectorInfo> response,
      final ConnectResponse<ConnectorStateInfo> status
  ) {
    if (response.error().isPresent() || status.error().isPresent()) {
      return new SimpleConnectorInfo(name, null, null, status.datum().get().connector().state());
    }

    final ConnectorInfo info = response.datum().get();
    return new SimpleConnectorInfo(
        name,
        info.type(),
        info.config().get(ConfigInfos.CONNECTOR_CLASS_CONFIG),
        summarizeState(status.datum().get())
    );
  }

  private static String summarizeState(final ConnectorStateInfo connectorState) {
    if (!connectorState.connector().state().equals(RUNNING)) {
      return connectorState.connector().state();
    }

    final long numRunningTasks = connectorState.tasks()
        .stream()
        .map(AbstractState::state)
        .filter(RUNNING::equals)
        .count();

    final String status = connectorState.tasks().size() > 0 && numRunningTasks == 0
        ? "WARNING"
        : "RUNNING";
    return String.format("%s (%s/%s tasks RUNNING)",
        status,
        numRunningTasks,
        connectorState.tasks().size());
  }
}
