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
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.DescribeConnector;
import io.confluent.ksql.rest.entity.ConnectorDescription;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Optional;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;

public class DescribeConnectorExecutor {

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<DescribeConnector> configuredStatement,
      final KsqlExecutionContext ksqlExecutionContext,
      final ServiceContext serviceContext
  ) {
    final String connectorName = configuredStatement
        .getStatement()
        .getConnectorName();
    final ConnectResponse<ConnectorStateInfo> response = serviceContext
        .getConnectClient()
        .status(connectorName);

    if (response.error().isPresent()) {
      return Optional.of(
          new ErrorEntity(
              configuredStatement.getStatementText(),
              response.error().get())
      );
    }

    // if error is missing, this must be present
    // noinspection OptionalGetWithoutIsPresent
    final ConnectorStateInfo info = response.datum().get();
    final ConnectorDescription description = new ConnectorDescription(
        configuredStatement.getStatementText(),
        info,
        ImmutableList.of() // TODO(agavra): get source descriptions after rebasing
    );

    return Optional.of(description);
  }
}
