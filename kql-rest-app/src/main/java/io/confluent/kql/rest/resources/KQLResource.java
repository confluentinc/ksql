package io.confluent.kql.rest.resources;

import io.confluent.kql.KQLEngine;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.parser.KQLParser;
import io.confluent.kql.parser.SqlBaseParser;
import io.confluent.kql.parser.tree.CreateStream;
import io.confluent.kql.parser.tree.CreateStreamAsSelect;
import io.confluent.kql.parser.tree.CreateTable;
import io.confluent.kql.parser.tree.CreateTableAsSelect;
import io.confluent.kql.parser.tree.CreateTopic;
import io.confluent.kql.parser.tree.ListStreams;
import io.confluent.kql.parser.tree.ListTables;
import io.confluent.kql.parser.tree.ListTopics;
import io.confluent.kql.parser.tree.ShowColumns;
import io.confluent.kql.parser.tree.ShowQueries;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.parser.tree.TerminateQuery;
import io.confluent.kql.planner.plan.KQLStructuredDataOutputNode;
import io.confluent.kql.rest.computation.CommandRunner;
import io.confluent.kql.rest.computation.QueryHandler;
import io.confluent.kql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.kql.util.QueryMetadata;
import io.confluent.kql.util.SchemaUtil;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.kafka.connect.data.Field;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Path("/kql")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class KQLResource {

  private static final org.slf4j.Logger log = LoggerFactory.getLogger(KQLResource.class);

  private final KQLEngine kqlEngine;
  private final CommandRunner commandRunner;
  private final QueryHandler queryHandler;

  public KQLResource(
      KQLEngine kqlEngine,
      CommandRunner commandRunner,
      QueryHandler queryHandler
  ) {
    this.kqlEngine = kqlEngine;
    this.commandRunner = commandRunner;
    this.queryHandler = queryHandler;
  }

  @POST
  public Response handleKQLStatements(KQLJsonRequest request) {
    try {
      List<Statement> parsedStatements = kqlEngine.getStatements(request.getKql());
      List<String> statementStrings = getStatementStrings(request.getKql());
      if (parsedStatements.size() != statementStrings.size()) {
        throw new Exception(String.format(
            "Size of parsed statements and statement strings differ; %d vs. %d, respectively",
            parsedStatements.size(),
            statementStrings.size()
        ));
      }
      JsonArrayBuilder result = Json.createArrayBuilder();
      for (int i = 0; i < parsedStatements.size(); i++) {
        String statementString = statementStrings.get(i);
        try {
          result.add(executeStatement(statementString, parsedStatements.get(i)));
        } catch (Exception exception) {
          result.add(KQLErrorResponse.stackTraceJson(exception));
        }
      }
      return Response.ok(result.build().toString()).build();
    } catch (Exception exception) {
      return KQLErrorResponse.stackTraceResponse(exception);
    }
  }

  private List<String> getStatementStrings(String kqlString) {
    List<SqlBaseParser.SingleStatementContext> statementContexts = new KQLParser().getStatements(kqlString);
    List<String> result = new ArrayList<>(statementContexts.size());
    for (SqlBaseParser.SingleStatementContext statementContext : statementContexts) {
      // Taken from http://stackoverflow.com/questions/16343288/how-do-i-get-the-original-text-that-an-antlr4-rule-matched
      CharStream charStream = statementContext.start.getInputStream();
      result.add(
          charStream.getText(
              new Interval(
                  statementContext.start.getStartIndex(),
                  statementContext.stop.getStopIndex()
              )
          )
      );
    }
    return result;
  }

  private JsonObject executeStatement(String statementString, Statement statement) throws Exception {
    JsonObjectBuilder result = Json.createObjectBuilder();
    if (statement instanceof ListTopics) {
      result.add("topics", listTopics());
    } else if (statement instanceof ListStreams) {
      result.add("streams", listStreams());
    } else if (statement instanceof ListTables) {
      result.add("tables", listTables());
    } else if (statement instanceof ShowQueries) {
      result.add("queries", showQueries());
    } else if (statement instanceof ShowColumns) {
      result.add("description", describe(((ShowColumns) statement).getTable().getSuffix()));
    } else if (statement instanceof CreateTopic
            || statement instanceof CreateStream
            || statement instanceof CreateTable
            || statement instanceof CreateStreamAsSelect
            || statement instanceof CreateTableAsSelect
            || statement instanceof TerminateQuery
    ) {
      String commandId = commandRunner.distributeStatement(statementString);
      queryHandler.registerQueuedStatement(commandId);
      result.add("statement_id", commandId);
    } else {
      if (statement != null) {
        throw new Exception(String.format(
            "Cannot handle statement of type '%s'",
            statement.getClass().getSimpleName()
        ));
      } else if (statementString != null) {
        throw new Exception(String.format(
            "Unable to execute statement '%s'",
            statementString
        ));
      } else {
        throw new Exception("Unable to execute statement");
      }
    }
    return result.build();
  }

  private JsonObjectBuilder formatTopicAsJson(KQLTopic kqlTopic) {
    JsonObjectBuilder result = Json.createObjectBuilder();
    result.add("kafka_topic", kqlTopic.getKafkaTopicName());
    result.add("format", kqlTopic.getKqlTopicSerDe().getSerDe().toString());
    if (kqlTopic.getKqlTopicSerDe() instanceof KQLAvroTopicSerDe) {
      result.add("avro_schema", ((KQLAvroTopicSerDe) kqlTopic.getKqlTopicSerDe()).getSchemaString());
    }
    return result;
  }

  private JsonObject listTopics() {
    JsonObjectBuilder result = Json.createObjectBuilder();
    Map<String, KQLTopic> topicMap = kqlEngine.getMetaStore().getAllKQLTopics();
    for (Map.Entry<String, KQLTopic> topicEntry : topicMap.entrySet()) {
      result.add(topicEntry.getKey(), formatTopicAsJson(topicEntry.getValue()).build());
    }
    return result.build();
  }

  private JsonObjectBuilder formatDataSourceAsJson(StructuredDataSource dataSource) {
    JsonObjectBuilder result = Json.createObjectBuilder();
    result.add("kql_topic", dataSource.getKqlTopic().getName());
    result.add("key", dataSource.getKeyField().name());
    result.add("format", dataSource.getKqlTopic().getKqlTopicSerDe().getSerDe().toString());
    return result;
  }

  // Only shows queries running on the current machine, not across the entire cluster
  private JsonObject showQueries() {
    JsonObjectBuilder result = Json.createObjectBuilder();
    for (Map.Entry<String, QueryMetadata> queryEntry : queryHandler.getLiveQueries().entrySet()) {
      KQLStructuredDataOutputNode kqlStructuredDataOutputNode =
          (KQLStructuredDataOutputNode) queryEntry.getValue().getQueryOutputNode();
      JsonObjectBuilder query = Json.createObjectBuilder();
      query.add("query", queryEntry.getValue().getQueryId());
      query.add("kafka_topic", kqlStructuredDataOutputNode.getKafkaTopicName());
      result.add(queryEntry.getKey(), query.build());
    }
    return result.build();
  }

  private JsonObject describe(String name) throws Exception {
    JsonObjectBuilder result = Json.createObjectBuilder();
    StructuredDataSource dataSource = kqlEngine.getMetaStore().getSource(name);
    if (dataSource == null) {
      throw new Exception(String.format("Could not find topic '%s' in the metastore", name));
    }
    result.add("key", dataSource.getKeyField().name());
    result.add("type", dataSource.getDataSourceType().toString());
    JsonObjectBuilder fields = Json.createObjectBuilder();
    for (Field schemaField : dataSource.getSchema().fields()) {
      String fieldName = schemaField.name();
      String type = SchemaUtil.TYPE_MAP.get(schemaField.schema().type().getName().toUpperCase()).toUpperCase();
      fields.add(fieldName, type);
    }
    result.add("schema", fields);
    return result.build();
  }

  // TODO: Stop showing streamed queries
  private JsonObject listStreams() {
    JsonObjectBuilder result = Json.createObjectBuilder();
    Map<String, StructuredDataSource> allDataSources = kqlEngine.getMetaStore().getAllStructuredDataSources();
    for (Map.Entry<String, StructuredDataSource> dataSourceEntry : allDataSources.entrySet()) {
      if (dataSourceEntry.getValue() instanceof KQLStream) {
        result.add(dataSourceEntry.getKey(), formatDataSourceAsJson(dataSourceEntry.getValue()).build());
      }
    }
    return result.build();
  }

  // TODO: Stop showing streamed queries
  private JsonObject listTables() {
    JsonObjectBuilder result = Json.createObjectBuilder();
    Map<String, StructuredDataSource> allDataSources = kqlEngine.getMetaStore().getAllStructuredDataSources();
    for (Map.Entry<String, StructuredDataSource> dataSourceEntry : allDataSources.entrySet()) {
      if (dataSourceEntry.getValue() instanceof KQLTable) {
        KQLTable kqlTable = (KQLTable) dataSourceEntry.getValue();
        JsonObjectBuilder datasourceInfo = formatDataSourceAsJson(kqlTable);
        datasourceInfo.add("statestore", kqlTable.getStateStoreName());
        result.add(dataSourceEntry.getKey(), datasourceInfo.build());
      }
    }
    return result.build();
  }
}
