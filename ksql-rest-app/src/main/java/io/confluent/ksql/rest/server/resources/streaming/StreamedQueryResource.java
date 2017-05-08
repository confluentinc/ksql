/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.resources.streaming;

import io.confluent.ksql.KQLEngine;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.resources.KQLJsonRequest;
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

  private final KQLEngine kqlEngine;
  private final StatementParser statementParser;
  private final long disconnectCheckInterval;

  public StreamedQueryResource(
      KQLEngine kqlEngine,
      StatementParser statementParser,
      long disconnectCheckInterval
  ) {
    this.kqlEngine = kqlEngine;
    this.statementParser = statementParser;
    this.disconnectCheckInterval = disconnectCheckInterval;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response streamQuery(KQLJsonRequest request) throws Exception {
    String kql = Objects.requireNonNull(request.getKql(), "\"ksql\" field must be given");
    Statement statement = statementParser.parseSingleStatement(kql);
    if (statement instanceof Query) {
      QueryStreamWriter queryStreamWriter =
          new QueryStreamWriter(kqlEngine, disconnectCheckInterval, kql);
      log.info("Streaming query '{}'", kql);
      return Response.ok().entity(queryStreamWriter).type(MediaType.APPLICATION_JSON).build();
    } else {
      throw new Exception(
          String.format("Statement type `%s' not supported for this resource", statement.getClass().getName())
      );
    }
  }
}
