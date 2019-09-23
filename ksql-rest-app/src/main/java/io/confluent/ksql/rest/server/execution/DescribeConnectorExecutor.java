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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.connect.Connector;
import io.confluent.ksql.connect.supported.Connectors;
import io.confluent.ksql.parser.tree.DescribeConnector;
import io.confluent.ksql.rest.entity.ConnectorDescription;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionFactory;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DescribeConnectorExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(DescribeConnectorExecutor.class);

  private final Function<ConnectorInfo, Optional<Connector>> connectorFactory;

  public DescribeConnectorExecutor() {
    this(Connectors::from);
  }

  @VisibleForTesting
  DescribeConnectorExecutor(final Function<ConnectorInfo, Optional<Connector>> connectorFactory) {
    this.connectorFactory = connectorFactory;
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public Optional<KsqlEntity> execute(
      final ConfiguredStatement<DescribeConnector> configuredStatement,
      final KsqlExecutionContext ksqlExecutionContext,
      final ServiceContext serviceContext
  ) {
    final String connectorName = configuredStatement
        .getStatement()
        .getConnectorName();

    final ConnectResponse<ConnectorStateInfo> statusResponse = serviceContext
        .getConnectClient()
        .status(connectorName);
    if (statusResponse.error().isPresent()) {
      return Optional.of(
          new ErrorEntity(
              configuredStatement.getStatementText(),
              statusResponse.error().get())
      );
    }

    final ConnectResponse<ConnectorInfo> infoResponse = serviceContext
        .getConnectClient()
        .describe(connectorName);
    if (infoResponse.error().isPresent()) {
      return Optional.of(
          new ErrorEntity(
              configuredStatement.getStatementText(),
              infoResponse.error().get())
      );
    }

    final ConnectorStateInfo status = statusResponse.datum().get();
    final ConnectorInfo info = infoResponse.datum().get();

    final Optional<Connector> connector = connectorFactory.apply(info);
    final List<SourceDescription> sources;
    if (connector.isPresent()) {
      sources = ksqlExecutionContext
          .getMetaStore()
          .getAllDataSources()
          .values()
          .stream()
          .filter(source -> connector.get().matches(source.getKafkaTopicName()))
          .map(source -> SourceDescriptionFactory.create(
              source,
              false,
              source.getKsqlTopic().getValueFormat().getFormat().name(),
              ImmutableList.of(),
              ImmutableList.of(),
              Optional.empty()))
          .collect(Collectors.toList());
    } else {
      sources = ImmutableList.of();
    }

    List<KsqlWarning> warnings;
    List<String> topics;
    try {
      topics = getTopics(serviceContext.getAdminClient(), connector);
      warnings = ImmutableList.of();
    } catch (Exception e) {
      topics = ImmutableList.of();
      warnings = ImmutableList.of(
          new KsqlWarning("Could not list related topics due to " + e.getMessage())
      );
    }

    final ConnectorDescription description = new ConnectorDescription(
        configuredStatement.getStatementText(),
        info.config().get(ConnectorConfig.CONNECTOR_CLASS_CONFIG),
        status,
        sources,
        topics,
        warnings
    );

    return Optional.of(description);
  }

  private List<String> getTopics(final Admin admin, final Optional<Connector> connector)
      throws Exception {
    if (!connector.isPresent()) {
      return ImmutableList.of();
    }

    return admin.listTopics().names().get(5, TimeUnit.SECONDS)
        .stream()
        .filter(connector.get()::matches)
        .collect(Collectors.toList());
  }
}
