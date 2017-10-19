/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.resources.streaming;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.StatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
public class StreamedQueryResource {
  private static final Logger log = LoggerFactory.getLogger(StreamedQueryResource.class);

  private final KsqlEngine ksqlEngine;
  private final StatementParser statementParser;
  private final long disconnectCheckInterval;

  public StreamedQueryResource(
      KsqlEngine ksqlEngine,
      StatementParser statementParser,
      long disconnectCheckInterval
  ) {
    this.ksqlEngine = ksqlEngine;
    this.statementParser = statementParser;
    this.disconnectCheckInterval = disconnectCheckInterval;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response streamQuery(KsqlRequest request) throws Exception {
    String ksql = Objects.requireNonNull(request.getKsql(), "\"ksql\" field must be given");
    Map<String, Object> clientLocalProperties =
        Optional.ofNullable(request.getStreamsProperties()).orElse(Collections.emptyMap());
    Statement statement = statementParser.parseSingleStatement(ksql);

    if (statement instanceof Query) {
      QueryStreamWriter queryStreamWriter =
          new QueryStreamWriter(ksqlEngine, disconnectCheckInterval, ksql, clientLocalProperties);
      log.info("Streaming query '{}'", ksql);
      return Response.ok().entity(queryStreamWriter).build();

    } else if (statement instanceof PrintTopic) {
      TopicStreamWriter topicStreamWriter = getTopicStreamWriter(clientLocalProperties, (PrintTopic) statement);
      return Response.ok().entity(topicStreamWriter).build();
    } else {
      throw new Exception(String.format(
          "Statement type `%s' not supported for this resource",
          statement.getClass().getName()
      ));
    }
  }

  private TopicStreamWriter getTopicStreamWriter(final Map<String, Object> clientLocalProperties, final PrintTopic printTopic) {
    String topicName = printTopic.getTopic().toString();
    Long interval =
        Optional.ofNullable(printTopic.getIntervalValue()).map(LongLiteral::getValue).orElse(1L);
    KsqlTopic ksqlTopic = ksqlEngine.getMetaStore().getTopic(topicName);
    Objects.requireNonNull(
        ksqlTopic,
        String.format("Could not find topic '%s' in the metastore", topicName)
    );
    Map<String, Object> properties = ksqlEngine.getKsqlConfigProperties();
    properties.putAll(clientLocalProperties);
    TopicStreamWriter topicStreamWriter = new TopicStreamWriter(
        properties,
        ksqlTopic,
        interval,
        disconnectCheckInterval,
        printTopic.getFromBeginning()
    );
    log.info("Printing topic '{}'", topicName);
    return topicStreamWriter;
  }
}
