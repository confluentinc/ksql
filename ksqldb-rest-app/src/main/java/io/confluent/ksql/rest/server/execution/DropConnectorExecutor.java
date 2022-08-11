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

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.DropConnector;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.DropConnectorEntity;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.WarningEntity;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Optional;
import org.apache.hc.core5.http.HttpStatus;

public final class DropConnectorExecutor {

  private DropConnectorExecutor() {
  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<DropConnector> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final String connectorName = statement.getStatement().getConnectorName();
    final boolean ifExists = statement.getStatement().getIfExists();
    final ConnectResponse<String> response =
        serviceContext.getConnectClient().delete(connectorName);

    if (response.error().isPresent()) {
      if (ifExists && response.httpCode() == HttpStatus.SC_NOT_FOUND) {
        return StatementExecutorResponse.handled(Optional.of(
            new WarningEntity(statement.getMaskedStatementText(),
                "Connector '" + connectorName + "' does not exist.")));
      } else {
        final String errorMsg = "Failed to drop connector: " + response.error().get();
        throw new KsqlRestException(EndpointResponse.create()
            .status(response.httpCode())
            .entity(new KsqlErrorMessage(Errors.toErrorCode(response.httpCode()), errorMsg))
            .build()
        );
      }
    }

    return StatementExecutorResponse.handled(Optional.of(
        new DropConnectorEntity(statement.getMaskedStatementText(), connectorName)));
  }
}
