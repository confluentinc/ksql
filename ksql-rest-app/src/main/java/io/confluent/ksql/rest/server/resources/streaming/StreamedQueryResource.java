/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.resources.streaming;

import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.resources.KSQLJsonRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Objects;

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
  public Response streamQuery(KSQLJsonRequest request) throws Exception {
    String ksql = Objects.requireNonNull(request.getKsql(), "\"ksql\" field must be given");
    Statement statement = statementParser.parseSingleStatement(ksql);
    if (statement instanceof Query) {
      QueryStreamWriter queryStreamWriter =
          new QueryStreamWriter(ksqlEngine, disconnectCheckInterval, ksql);
      log.info("Streaming query '{}'", ksql);
      return Response.ok().entity(queryStreamWriter).type(MediaType.APPLICATION_JSON).build();
    } else {
      throw new Exception(
          String.format("Statement type `%s' not supported for this resource", statement.getClass().getName())
      );
    }
  }
}
