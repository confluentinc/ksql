/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.resources.streaming;

import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.metastore.KSQLTopic;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.KSQLRequest;
import io.confluent.ksql.rest.server.StatementParser;
import org.apache.kafka.streams.StreamsConfig;
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
import java.util.Optional;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
public class StreamedQueryResource {
  private static final Logger log = LoggerFactory.getLogger(StreamedQueryResource.class);

  private final KSQLEngine ksqlEngine;
  private final StatementParser statementParser;
  private final long disconnectCheckInterval;

  public StreamedQueryResource(
      KSQLEngine ksqlEngine,
      StatementParser statementParser,
      long disconnectCheckInterval
  ) {
    this.ksqlEngine = ksqlEngine;
    this.statementParser = statementParser;
    this.disconnectCheckInterval = disconnectCheckInterval;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response streamQuery(KSQLRequest request) throws Exception {
    String ksql = Objects.requireNonNull(request.getKsql(), "\"ksql\" field must be given");
    Statement statement = statementParser.parseSingleStatement(ksql);
    if (statement instanceof Query) {
      QueryStreamWriter queryStreamWriter =
          new QueryStreamWriter(ksqlEngine, disconnectCheckInterval, ksql);
      log.info("Streaming query '{}'", ksql);
      return Response.ok().entity(queryStreamWriter).build();
    } else if (statement instanceof PrintTopic) {
      PrintTopic printTopic = (PrintTopic) statement;
      String topicName = printTopic.getTopic().toString();
      Long interval = Optional.ofNullable(printTopic.getIntervalValue()).map(LongLiteral::getValue).orElse(1L);
      KSQLTopic ksqlTopic = ksqlEngine.getMetaStore().getTopic(printTopic.getTopic().toString());
      Objects.requireNonNull(ksqlTopic, String.format("Could not find topic '%s' in the metastore", topicName));
      Map<String, Object> consumerProperties =
          new StreamsConfig(ksqlEngine.getStreamsProperties()).getRestoreConsumerConfigs(null);
      TopicStreamWriter topicStreamWriter =
          new TopicStreamWriter(consumerProperties, ksqlTopic, interval, disconnectCheckInterval);
      log.info("Printing topic '{}'", topicName);
      return Response.ok().entity(topicStreamWriter).build();
    } else {
      throw new Exception(
          String.format("Statement type `%s' not supported for this resource", statement.getClass().getName())
      );
    }
  }
}
