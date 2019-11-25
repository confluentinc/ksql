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
import io.confluent.ksql.parser.tree.DropConnector;
import io.confluent.ksql.rest.entity.DropConnectorEntity;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.List;
import java.util.Map;

public final class DropConnectorExecutor {

  private DropConnectorExecutor() { }

  public static List<? extends KsqlEntity> execute(
      final ConfiguredStatement<DropConnector> statement,
      final Map<String, ?> sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final String connectorName = statement.getStatement().getConnectorName();
    final ConnectResponse<String> response =
        serviceContext.getConnectClient().delete(connectorName);

    if (response.error().isPresent()) {
      return ImmutableList
          .of(new ErrorEntity(statement.getStatementText(), response.error().get()));
    }

    return ImmutableList.of(new DropConnectorEntity(statement.getStatementText(), connectorName));
  }
}
