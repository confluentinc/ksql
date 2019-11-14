/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.resources.streaming;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.TableRowsEntity;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.server.resources.KsqlConfigurable;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.rest.util.CommandStoreUtil;
import io.confluent.ksql.rest.util.ErrorResponseUtil;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/query")
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
@Consumes({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class StreamedQueryResource implements KsqlConfigurable {

  private static final Logger log = LoggerFactory.getLogger(StreamedQueryResource.class);

  private final KsqlEngine ksqlEngine;
  private final StatementParser statementParser;
  private final CommandQueue commandQueue;
  private final Duration disconnectCheckInterval;
  private final Duration commandQueueCatchupTimeout;
  private final ObjectMapper objectMapper;
  private final ActivenessRegistrar activenessRegistrar;
  private final KsqlAuthorizationValidator authorizationValidator;
  private KsqlConfig ksqlConfig;

  public StreamedQueryResource(
      final KsqlEngine ksqlEngine,
      final CommandQueue commandQueue,
      final Duration disconnectCheckInterval,
      final Duration commandQueueCatchupTimeout,
      final ActivenessRegistrar activenessRegistrar,
      final KsqlAuthorizationValidator authorizationValidator
  ) {
    this(
        ksqlEngine,
        new StatementParser(ksqlEngine),
        commandQueue,
        disconnectCheckInterval,
        commandQueueCatchupTimeout,
        activenessRegistrar,
        authorizationValidator
    );
  }

  @VisibleForTesting
  StreamedQueryResource(
      final KsqlEngine ksqlEngine,
      final StatementParser statementParser,
      final CommandQueue commandQueue,
      final Duration disconnectCheckInterval,
      final Duration commandQueueCatchupTimeout,
      final ActivenessRegistrar activenessRegistrar,
      final KsqlAuthorizationValidator authorizationValidator
  ) {
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.statementParser = Objects.requireNonNull(statementParser, "statementParser");
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");
    this.disconnectCheckInterval =
        Objects.requireNonNull(disconnectCheckInterval, "disconnectCheckInterval");
    this.commandQueueCatchupTimeout =
        Objects.requireNonNull(commandQueueCatchupTimeout, "commandQueueCatchupTimeout");
    this.objectMapper = JsonMapper.INSTANCE.mapper;
    this.activenessRegistrar =
        Objects.requireNonNull(activenessRegistrar, "activenessRegistrar");
    this.authorizationValidator = authorizationValidator;
  }

  @Override
  public void configure(final KsqlConfig config) {
    if (!config.getKsqlStreamConfigProps().containsKey(StreamsConfig.APPLICATION_SERVER_CONFIG)) {
      throw new IllegalArgumentException("Need KS application server set");
    }

    ksqlConfig = config;
  }

  @POST
  public Response streamQuery(
      @Context final ServiceContext serviceContext,
      final KsqlRequest request
  ) {
    throwIfNotConfigured();

    activenessRegistrar.updateLastRequestTime();

    final PreparedStatement<?> statement = parseStatement(request);

    CommandStoreUtil.httpWaitForCommandSequenceNumber(
        commandQueue, request, commandQueueCatchupTimeout);

    return handleStatement(serviceContext, request, statement);
  }

  private void throwIfNotConfigured() {
    if (ksqlConfig == null) {
      throw new KsqlRestException(Errors.notReady());
    }
  }

  private PreparedStatement<?> parseStatement(final KsqlRequest request) {
    final String ksql = request.getKsql();
    if (ksql.trim().isEmpty()) {
      throw new KsqlRestException(Errors.badRequest("\"ksql\" field must be populated"));
    }

    try {
      return statementParser.parseSingleStatement(ksql);
    } catch (IllegalArgumentException | KsqlException e) {
      throw new KsqlRestException(Errors.badStatement(e, ksql));
    }
  }

  @SuppressWarnings("unchecked")
  private Response handleStatement(
      final ServiceContext serviceContext,
      final KsqlRequest request,
      final PreparedStatement<?> statement
  )  {
    try {
      authorizationValidator.checkAuthorization(
          serviceContext,
          ksqlEngine.getMetaStore(),
          statement.getStatement()
      );

      if (statement.getStatement() instanceof Query) {
        final PreparedStatement<Query> queryStmt = (PreparedStatement<Query>) statement;

        if (queryStmt.getStatement().isPullQuery()) {
          return handlePullQuery(
              serviceContext,
              queryStmt,
              request.getStreamsProperties()
          );
        }

        return handlePushQuery(
            serviceContext,
            queryStmt,
            request.getStreamsProperties()
        );
      }

      if (statement.getStatement() instanceof PrintTopic) {
        return handlePrintTopic(
            serviceContext,
            request.getStreamsProperties(),
            (PreparedStatement<PrintTopic>) statement);
      }

      return Errors.badRequest(String.format(
          "Statement type `%s' not supported for this resource",
          statement.getClass().getName()));
    } catch (final TopicAuthorizationException e) {
      return Errors.accessDeniedFromKafka(e);
    } catch (final KsqlException e) {
      return ErrorResponseUtil.generateResponse(
          e, Errors.badRequest(e));
    }
  }

  private Response handlePullQuery(
      final ServiceContext serviceContext,
      final PreparedStatement<Query> statement,
      final Map<String, Object> streamsProperties
  ) {
    final ConfiguredStatement<Query> configured =
        ConfiguredStatement.of(statement, streamsProperties, ksqlConfig);

    final TableRowsEntity entity = PullQueryExecutor
        .execute(configured, ksqlEngine, serviceContext);

    final StreamedRow header = StreamedRow.header(entity.getQueryId(), entity.getSchema());

    final List<StreamedRow> rows = entity.getRows().stream()
        .map(GenericRow::new)
        .map(StreamedRow::row)
        .collect(Collectors.toList());

    rows.add(0, header);

    final String data = rows.stream()
        .map(this::writeValueAsString)
        .collect(Collectors.joining("," + System.lineSeparator(), "[", "]"));

    return Response.ok().entity(data).build();
  }

  private Response handlePushQuery(
      final ServiceContext serviceContext,
      final PreparedStatement<Query> statement,
      final Map<String, Object> streamsProperties
  ) {
    final ConfiguredStatement<Query> configured =
        ConfiguredStatement.of(statement, streamsProperties, ksqlConfig);

    final QueryMetadata query = ksqlEngine.execute(serviceContext, configured)
        .getQuery()
        .get();

    if (!(query instanceof TransientQueryMetadata)) {
      throw new IllegalStateException(String.format(
          "Unexpected metadata type: expected TransientQueryMetadata, found %s instead",
          query.getClass()
      ));
    }

    final QueryStreamWriter queryStreamWriter = new QueryStreamWriter(
        (TransientQueryMetadata) query,
        disconnectCheckInterval.toMillis(),
        objectMapper);

    log.info("Streaming query '{}'", statement.getStatementText());
    return Response.ok().entity(queryStreamWriter).build();
  }

  private String writeValueAsString(final Object object) {
    try {
      return objectMapper.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private Response handlePrintTopic(
      final ServiceContext serviceContext,
      final Map<String, Object> streamProperties,
      final PreparedStatement<PrintTopic> statement
  ) {
    final PrintTopic printTopic = statement.getStatement();
    final String topicName = printTopic.getTopic();

    if (!serviceContext.getTopicClient().isTopicExists(topicName)) {
      final Collection<String> possibleAlternatives =
          findPossibleTopicMatches(topicName, serviceContext);

      final String reverseSuggestion = possibleAlternatives.isEmpty()
          ? ""
          : possibleAlternatives.stream()
              .map(name -> "\tprint " + name + ";")
              .collect(Collectors.joining(
              System.lineSeparator(),
              System.lineSeparator() + "Did you mean:" + System.lineSeparator(),
              ""
          ));

      throw new KsqlRestException(
          Errors.badRequest(
              "Could not find topic '" + topicName + "', "
                  + "or the KSQL user does not have permissions to list the topic. "
                  + "Topic names are case-sensitive."
                  + reverseSuggestion
          ));
    }

    final Map<String, Object> propertiesWithOverrides =
        new HashMap<>(ksqlConfig.getKsqlStreamConfigProps());
    propertiesWithOverrides.putAll(streamProperties);

    final TopicStreamWriter topicStreamWriter = TopicStreamWriter.create(
        serviceContext,
        propertiesWithOverrides,
        printTopic,
        disconnectCheckInterval
    );

    log.info("Printing topic '{}'", topicName);
    return Response.ok().entity(topicStreamWriter).build();
  }

  private static Collection<String> findPossibleTopicMatches(
      final String topicName,
      final ServiceContext serviceContext
  ) {
    return serviceContext.getTopicClient().listTopicNames().stream()
        .filter(name -> name.equalsIgnoreCase(topicName))
        .collect(Collectors.toSet());
  }
}


