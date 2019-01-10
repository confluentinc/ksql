/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.resources.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.rest.util.CommandStoreUtil;
import io.confluent.ksql.rest.util.JsonMapper;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/query")
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
@Consumes({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class StreamedQueryResource {

  private static final Logger log = LoggerFactory.getLogger(StreamedQueryResource.class);

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final ServiceContext serviceContext;
  private final StatementParser statementParser;
  private final CommandQueue commandQueue;
  private final Duration disconnectCheckInterval;
  private final Duration commandQueueCatchupTimeout;
  private final ObjectMapper objectMapper;
  private final ActivenessRegistrar activenessRegistrar;

  public StreamedQueryResource(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final StatementParser statementParser,
      final CommandQueue commandQueue,
      final Duration disconnectCheckInterval,
      final Duration commandQueueCatchupTimeout,
      final ActivenessRegistrar activenessRegistrar
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.statementParser = Objects.requireNonNull(statementParser, "statementParser");
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");
    this.disconnectCheckInterval =
        Objects.requireNonNull(disconnectCheckInterval, "disconnectCheckInterval");
    this.commandQueueCatchupTimeout =
        Objects.requireNonNull(commandQueueCatchupTimeout, "commandQueueCatchupTimeout");
    this.objectMapper = JsonMapper.INSTANCE.mapper;
    this.activenessRegistrar =
        Objects.requireNonNull(activenessRegistrar, "activenessRegistrar");
  }

  @POST
  public Response streamQuery(final KsqlRequest request) throws Exception {
    if (!ksqlEngine.isAcceptingStatements()) {
      return Errors.serverErrorForStatement(
          new KsqlException("Cluster has been terminated."),
          "The cluster has been terminated. No new request will be accepted.",
          new KsqlEntityList());
    }

    activenessRegistrar.updateLastRequestTime();

    final PreparedStatement<?> statement = parseStatement(request);

    CommandStoreUtil.httpWaitForCommandSequenceNumber(
        commandQueue, request, commandQueueCatchupTimeout);

    return handleStatement(request, statement);
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
      final KsqlRequest request,
      final PreparedStatement<?> statement
  ) throws Exception {
    try {
      if (statement.getStatement() instanceof Query) {
        return handleQuery((PreparedStatement<Query>) statement, request.getStreamsProperties());
      }

      if (statement.getStatement() instanceof PrintTopic) {
        return handlePrintTopic((PreparedStatement<PrintTopic>) statement);
      }

      return Errors.badRequest(String.format(
          "Statement type `%s' not supported for this resource",
          statement.getClass().getName()));
    } catch (final KsqlException e) {
      return Errors.badRequest(e);
    }
  }

  @SuppressWarnings("ConstantConditions")
  private Response handleQuery(
      final PreparedStatement<Query> statement,
      final Map<String, Object> streamsProperties
  ) throws Exception {
    final QueryMetadata query = ksqlEngine.execute(statement, ksqlConfig, streamsProperties).get();

    if (!(query instanceof QueuedQueryMetadata)) {
      throw new Exception(String.format(
          "Unexpected metadata type: expected QueuedQueryMetadata, found %s instead",
          query.getClass()
      ));
    }

    final QueryStreamWriter queryStreamWriter = new QueryStreamWriter(
        (QueuedQueryMetadata) query,
        disconnectCheckInterval.toMillis(),
        objectMapper);

    log.info("Streaming query '{}'", statement.getStatementText());
    return Response.ok().entity(queryStreamWriter).build();
  }

  private Response handlePrintTopic(final PreparedStatement<PrintTopic> statement) {
    final PrintTopic printTopic = statement.getStatement();
    final String topicName = printTopic.getTopic().toString();

    if (!serviceContext.getTopicClient().isTopicExists(topicName)) {
      throw new KsqlRestException(
          Errors.badRequest(String.format(
              "Could not find topic '%s', "
                  + "or the KSQL user does not have permissions to list the topic."
                  + System.lineSeparator()
                  + "KSQL will treat unquoted topic names as uppercase."
                  + System.lineSeparator()
                  + "To print a case-sensitive topic use quotes, for example: print \'Topic\';",
              topicName)));
    }

    final TopicStreamWriter topicStreamWriter = new TopicStreamWriter(
        serviceContext.getSchemaRegistryClient(),
        ksqlConfig.getKsqlStreamConfigProps(),
        topicName,
        printTopic.getIntervalValue(),
        disconnectCheckInterval,
        printTopic.getFromBeginning()
    );

    log.info("Printing topic '{}'", topicName);
    return Response.ok().entity(topicStreamWriter).build();
  }
}
