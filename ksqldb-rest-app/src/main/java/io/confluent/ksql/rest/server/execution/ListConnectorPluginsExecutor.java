/*
 * Copyright 2021 Confluent Inc.
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
import io.confluent.ksql.parser.tree.ListConnectorPlugins;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ConnectorPluginsList;
import io.confluent.ksql.rest.entity.SimpleConnectorPluginInfo;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.PluginInfo;

public final class ListConnectorPluginsExecutor {
  private final ConnectServerErrors connectErrorHandler;

  ListConnectorPluginsExecutor(final ConnectServerErrors connectErrorHandler) {
    this.connectErrorHandler = connectErrorHandler;
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public StatementExecutorResponse execute(
      final ConfiguredStatement<ListConnectorPlugins> configuredStatement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext ksqlExecutionContext,
      final ServiceContext serviceContext
  ) {
    final ConnectResponse<List<PluginInfo>> plugins =
        serviceContext.getConnectClient().connectorPlugins();
    if (plugins.error().isPresent()) {
      return StatementExecutorResponse.handled(connectErrorHandler.handle(
          configuredStatement, plugins));
    }

    final List<SimpleConnectorPluginInfo> pluginInfos = new ArrayList<>();
    for (final PluginInfo info : plugins.datum().get()) {
      pluginInfos.add(new SimpleConnectorPluginInfo(
          info.className(),
          ConnectorType.forValue(info.type()),
          info.version()
      ));
    }

    return StatementExecutorResponse.handled(Optional.of(
      new ConnectorPluginsList(
        configuredStatement.getMaskedStatementText(),
        Collections.emptyList(),
        pluginInfos
      )
    ));
  }
}
