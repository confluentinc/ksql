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
import io.confluent.ksql.parser.tree.CreateConnector;
import io.confluent.ksql.rest.entity.CreateConnectorEntity;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Optional;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;

public final class ConnectExecutor {

  private ConnectExecutor() { }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<CreateConnector> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final CreateConnector createConnector = statement.getStatement();
    final ConnectClient client = serviceContext.getConnectClient();

    final ConnectResponse<ConnectorInfo> response = client.create(
        createConnector.getName(),
        Maps.transformValues(createConnector.getConfig(), l -> l.getValue().toString()));

    if (response.datum().isPresent()) {
      return Optional.of(
          new CreateConnectorEntity(
              statement.getStatementText(),
              response.datum().get()
          )
      );
    }

    return response.error()
        .map(err -> new ErrorEntity(statement.getStatementText(), err));
  }
}
