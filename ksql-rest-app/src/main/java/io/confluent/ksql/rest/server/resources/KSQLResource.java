/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.metastore.KSQLStream;
import io.confluent.ksql.metastore.KSQLTable;
import io.confluent.ksql.metastore.KSQLTopic;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.CreateTopic;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.planner.plan.KSQLStructuredDataOutputNode;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.StatementExecutor;
import io.confluent.ksql.serde.avro.KSQLAvroTopicSerDe;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.SchemaUtil;
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
import java.util.Objects;

@Path("/ksql")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class KSQLResource {

  private static final org.slf4j.Logger log = LoggerFactory.getLogger(KSQLResource.class);

  private final KSQLEngine ksqlEngine;
  private final CommandStore commandStore;
  private final StatementExecutor statementExecutor;

  public KSQLResource(
      KSQLEngine ksqlEngine,
      CommandStore commandStore,
      StatementExecutor statementExecutor
  ) {
    this.ksqlEngine = ksqlEngine;
    this.commandStore = commandStore;
    this.statementExecutor = statementExecutor;
  }

  @POST
  public Response handleKSQLStatements(KSQLJsonRequest request) throws Exception {
    List<Statement> parsedStatements = ksqlEngine.getStatements(request.getKsql());
    List<String> statementStrings = getStatementStrings(request.getKsql());
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
        result.add(KSQLExceptionMapper.stackTraceJson(exception));
      }
    }
    return Response.ok(result.build().toString()).build();
  }

  public List<String> getStatementStrings(String ksqlString) {
    List<SqlBaseParser.SingleStatementContext> statementContexts = new KSQLParser().getStatements(ksqlString);
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
    } else if (statement instanceof ListQueries) {
      result.add("queries", showQueries());
    } else if (statement instanceof ShowColumns) {
      result.add("description", describe(((ShowColumns) statement).getTable().getSuffix()));
    } else if (statement instanceof SetProperty) {
      result.add("set_property", setProperty((SetProperty) statement));
    } else if (statement instanceof ListProperties) {
      result.add("properties", listProperties());
    } else if (statement instanceof CreateTopic
            || statement instanceof CreateStream
            || statement instanceof CreateTable
            || statement instanceof CreateStreamAsSelect
            || statement instanceof CreateTableAsSelect
            || statement instanceof TerminateQuery
    ) {
      CommandId commandId = commandStore.distributeStatement(statementString, statement);
      statementExecutor.registerQueuedStatement(commandId);
      result.add("statement_id", commandId.toString());
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

  private JsonObjectBuilder formatTopicAsJson(KSQLTopic ksqlTopic) {
    JsonObjectBuilder result = Json.createObjectBuilder();
    result.add("kafka_topic", ksqlTopic.getKafkaTopicName());
    result.add("format", ksqlTopic.getKsqlTopicSerDe().getSerDe().toString());
    if (ksqlTopic.getKsqlTopicSerDe() instanceof KSQLAvroTopicSerDe) {
      result.add("avro_schema", ((KSQLAvroTopicSerDe) ksqlTopic.getKsqlTopicSerDe()).getSchemaString());
    }
    return result;
  }

  private JsonObject listTopics() {
    JsonObjectBuilder result = Json.createObjectBuilder();
    Map<String, KSQLTopic> topicMap = ksqlEngine.getMetaStore().getAllKSQLTopics();
    for (Map.Entry<String, KSQLTopic> topicEntry : topicMap.entrySet()) {
      result.add(topicEntry.getKey(), formatTopicAsJson(topicEntry.getValue()).build());
    }
    return result.build();
  }

  private JsonObjectBuilder formatDataSourceAsJson(StructuredDataSource dataSource) {
    JsonObjectBuilder result = Json.createObjectBuilder();
    result.add("ksql_topic", dataSource.getKsqlTopic().getName());
    result.add("key", (dataSource.getKeyField() != null) ? dataSource.getKeyField().name() :
                      "null");
    result.add("format", dataSource.getKsqlTopic().getKsqlTopicSerDe().getSerDe().toString());
    return result;
  }

  // Only shows queries running on the current machine, not across the entire cluster
  private JsonObject showQueries() {
    JsonObjectBuilder result = Json.createObjectBuilder();

    for (PersistentQueryMetadata persistentQueryMetadata : ksqlEngine.getPersistentQueries().values()) {
      KSQLStructuredDataOutputNode ksqlStructuredDataOutputNode =
          (KSQLStructuredDataOutputNode) persistentQueryMetadata.getOutputNode();

      JsonObjectBuilder persistentQuery = Json.createObjectBuilder();

      persistentQuery.add("query", persistentQueryMetadata.getStatementString());
      persistentQuery.add("kafka_topic", ksqlStructuredDataOutputNode.getKafkaTopicName());

      result.add(Long.toString(persistentQueryMetadata.getId()), persistentQuery.build());
    }

    return result.build();
  }

  private JsonObject describe(String name) throws Exception {
    JsonObjectBuilder result = Json.createObjectBuilder();
    StructuredDataSource dataSource = ksqlEngine.getMetaStore().getSource(name);
    if (dataSource == null) {
      throw new Exception(String.format("Could not find topic '%s' in the metastore", name));
    }
    result.add("key", (dataSource.getKeyField() != null) ? dataSource.getKeyField().name() :
                      "null");
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

  // TODO: Right now properties can only be set for a single node. Do we want to distribute this?
  private JsonObject setProperty(SetProperty setProperty) {
    JsonObjectBuilder result = Json.createObjectBuilder();
    ksqlEngine.setStreamsProperty(setProperty.getPropertyName(), setProperty.getPropertyValue());
    result.add("property", setProperty.getPropertyName());
    result.add("value", setProperty.getPropertyValue());
    return result.build();
  }

  private JsonObject listProperties() {
    JsonObjectBuilder result = Json.createObjectBuilder();
    for (Map.Entry<String, Object> propertyEntry : ksqlEngine.getStreamsProperties().entrySet()) {
      result.add(propertyEntry.getKey(), Objects.toString(propertyEntry.getValue()));
    }
    return result.build();
  }

  private JsonObject listStreams() {
    JsonObjectBuilder result = Json.createObjectBuilder();
    Map<String, StructuredDataSource> allDataSources = ksqlEngine.getMetaStore().getAllStructuredDataSources();
    for (Map.Entry<String, StructuredDataSource> dataSourceEntry : allDataSources.entrySet()) {
      if (dataSourceEntry.getValue() instanceof KSQLStream) {
        result.add(dataSourceEntry.getKey(), formatDataSourceAsJson(dataSourceEntry.getValue()).build());
      }
    }
    return result.build();
  }

  private JsonObject listTables() {
    JsonObjectBuilder result = Json.createObjectBuilder();
    Map<String, StructuredDataSource> allDataSources = ksqlEngine.getMetaStore().getAllStructuredDataSources();
    for (Map.Entry<String, StructuredDataSource> dataSourceEntry : allDataSources.entrySet()) {
      if (dataSourceEntry.getValue() instanceof KSQLTable) {
        KSQLTable ksqlTable = (KSQLTable) dataSourceEntry.getValue();
        JsonObjectBuilder datasourceInfo = formatDataSourceAsJson(ksqlTable);
        datasourceInfo.add("statestore", ksqlTable.getStateStoreName());
        result.add(dataSourceEntry.getKey(), datasourceInfo.build());
      }
    }
    return result.build();
  }
}
