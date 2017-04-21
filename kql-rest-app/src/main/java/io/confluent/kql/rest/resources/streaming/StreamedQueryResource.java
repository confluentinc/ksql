/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest.resources.streaming;

import io.confluent.kql.KQLEngine;
import io.confluent.kql.parser.tree.Query;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.rest.StatementParser;
import io.confluent.kql.rest.TopicUtil;
import io.confluent.kql.rest.resources.KQLErrorResponse;
import io.confluent.kql.rest.resources.KQLJsonRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
public class StreamedQueryResource {
  private static final Logger log = LoggerFactory.getLogger(StreamedQueryResource.class);

  private final KQLEngine kqlEngine;
  private final TopicUtil topicUtil;
  private final String nodeId;
  private final StatementParser statementParser;
  private final AtomicInteger queriesCreated;
  private final long disconnectCheckInterval;
  private final Map<String, Object> streamsProperties;

  public StreamedQueryResource(
      KQLEngine kqlEngine,
      TopicUtil topicUtil,
      String nodeId,
      StatementParser statementParser,
      long disconnectCheckInterval,
      Map<String, Object> streamsProperties) {
    this(kqlEngine, topicUtil, nodeId, statementParser, disconnectCheckInterval, streamsProperties, 0);
  }

  public StreamedQueryResource(
      KQLEngine kqlEngine,
      TopicUtil topicUtil,
      String nodeId,
      StatementParser statementParser,
      long disconnectCheckInterval,
      Map<String, Object> streamsProperties,
      int queriesCreated
  ) {
    this.kqlEngine = kqlEngine;
    this.topicUtil = topicUtil;
    this.nodeId = nodeId;
    this.statementParser = statementParser;
    this.disconnectCheckInterval = disconnectCheckInterval;
    this.streamsProperties = streamsProperties;
    this.queriesCreated = new AtomicInteger(queriesCreated);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response performQuery(KQLJsonRequest request) {
    try {
      String kql = Objects.requireNonNull(request.getKql(), "\"kql\" field must be given");
      Statement statement = statementParser.parseSingleStatement(kql);
      if (statement instanceof Query) {
        String streamName =
            String.format("%s_streamed_query_%d", nodeId, queriesCreated.incrementAndGet()).toUpperCase();
        topicUtil.ensureTopicExists(streamName);
        log.info("Assigning name '{}' to streamed query \"{}\"", streamName, kql);
        QueryStreamWriter queryStreamWriter =
            new QueryStreamWriter(kqlEngine, kql, disconnectCheckInterval, streamsProperties, streamName);
        return Response.ok().entity(queryStreamWriter).type(MediaType.APPLICATION_JSON).build();
      } else {
        throw new Exception(
            String.format("Statement type `%s' not supported for this resource", statement.getClass().getName())
        );
      }
    } catch (Exception exception) {
      return KQLErrorResponse.stackTraceResponse(exception);
    }
  }
}
