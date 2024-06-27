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
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ConnectorDescription;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionFactory;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DescribeConnectorExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(DescribeConnectorExecutor.class);

  @VisibleForTesting
  static final String TOPICS_KEY = "topics";

  private final Function<ConnectorInfo, Optional<Connector>> connectorFactory;

  public DescribeConnectorExecutor() {
    this(Connectors::from);
  }

  @VisibleForTesting
  DescribeConnectorExecutor(final Function<ConnectorInfo, Optional<Connector>> connectorFactory) {
    this.connectorFactory = connectorFactory;
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public StatementExecutorResponse execute(
      final ConfiguredStatement<DescribeConnector> configuredStatement,
      final SessionProperties sessionProperties,
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
      final String errorMsg = "Failed to query connector status: " + statusResponse.error().get();
      throw new KsqlRestException(EndpointResponse.create()
          .status(statusResponse.httpCode())
          .entity(new KsqlErrorMessage(Errors.toErrorCode(statusResponse.httpCode()), errorMsg))
          .build()
      );
    }

    final ConnectResponse<ConnectorInfo> infoResponse = serviceContext
        .getConnectClient()
        .describe(connectorName);
    if (infoResponse.error().isPresent()) {
      final String errorMsg = "Failed to describe connector: " + infoResponse.error().get();
      throw new KsqlRestException(EndpointResponse.create()
          .status(infoResponse.httpCode())
          .entity(new KsqlErrorMessage(Errors.toErrorCode(infoResponse.httpCode()), errorMsg))
          .build()
      );
    }

    final ConnectorStateInfo status = statusResponse.datum().get();
    final ConnectorInfo info = infoResponse.datum().get();
    final Optional<Connector> connector = connectorFactory.apply(info);
    final List<KsqlWarning> warnings;
    final List<String> topics;
    if (connector.isPresent()) {
      // Small optimization. If a connector's info is not found in the response, don't query for
      // active topics with the given connectorName
      final ConnectResponse<Map<String, Map<String, List<String>>>> topicsResponse = serviceContext
          .getConnectClient()
          .topics(connectorName);
      // topics endpoint is relatively new (KAFKA-9422), so 404 here is expected behavior for older
      // Connect versions. Rather than showing a scary warning to the user, we just log it to the
      // server logs.
      if (topicsResponse.error().isPresent()
          && topicsResponse.httpCode() == HttpStatus.SC_NOT_FOUND) {
        topics = ImmutableList.of();
        warnings = ImmutableList.of();
        LOG.warn("Could not list related topics due to error: " + topicsResponse.error().get());
      } else if (topicsResponse.error().isPresent()) {
        topics = ImmutableList.of();
        warnings = ImmutableList.of(
            new KsqlWarning("Could not list related topics due to error: "
                + topicsResponse.error().get()));
      } else {
        topics = topicsResponse.datum()
            .get()
            .get(connectorName)
            .getOrDefault(TOPICS_KEY, ImmutableList.of());
        warnings = ImmutableList.of();
      }
    } else {
      topics = ImmutableList.of();
      warnings = ImmutableList.of();
    }

    final List<SourceDescription> sources;
    if (connector.isPresent()) {
      sources = ksqlExecutionContext
          .getMetaStore()
          .getAllDataSources()
          .values()
          .stream()
          .filter(source -> topics.contains(source.getKafkaTopicName()))
          .map(source -> SourceDescriptionFactory.create(
                  source,
                  false,
                  ImmutableList.of(),
                  ImmutableList.of(),
                  Optional.empty(),
                  ImmutableList.of(),
                  ImmutableList.of(),
                  ksqlExecutionContext.metricCollectors()
              )
          )
          .collect(Collectors.toList());
    } else {
      sources = ImmutableList.of();
    }

    final ConnectorDescription description = new ConnectorDescription(
        configuredStatement.getMaskedStatementText(),
        info.config().get(ConnectorConfig.CONNECTOR_CLASS_CONFIG),
        status,
        sources,
        topics,
        warnings
    );

    return StatementExecutorResponse.handled(Optional.of(description));
  }
}
