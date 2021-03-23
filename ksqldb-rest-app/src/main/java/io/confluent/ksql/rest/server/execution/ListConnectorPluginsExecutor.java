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
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.SimpleConnectorPluginInfo;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorPluginInfo;

public final class ListConnectorPluginsExecutor {
  private ListConnectorPluginsExecutor() {
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<ListConnectorPlugins> configuredStatement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext ksqlExecutionContext,
      final ServiceContext serviceContext
  ) {
    final ConnectResponse<List<ConnectorPluginInfo>> plugins =
        serviceContext.getConnectClient().connectorPlugins();
    if (plugins.error().isPresent()) {
      return Optional.of(new ErrorEntity(
        configuredStatement.getStatementText(),
        plugins.error().get()
      ));
    }

    final List<SimpleConnectorPluginInfo> pluginInfos = new ArrayList<>();
    final List<KsqlWarning> warnings = new ArrayList<>();

    for (final ConnectorPluginInfo info : plugins.datum().get()) {
      pluginInfos.add(new SimpleConnectorPluginInfo(
          info.className(),
          info.type(),
          info.version()
      ));
    }

    return Optional.of(
      new ConnectorPluginsList(
        configuredStatement.getStatementText(),
         warnings,
        pluginInfos
      )
    );
  }
}
