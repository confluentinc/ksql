/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.metastore.KSQLStream;
import io.confluent.ksql.metastore.KSQLTable;
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
import io.confluent.ksql.rest.entity.CommandIdEntity;
import io.confluent.ksql.rest.entity.ErrorMessageEntity;
import io.confluent.ksql.rest.entity.KSQLEntity;
import io.confluent.ksql.rest.entity.KSQLEntityList;
import io.confluent.ksql.rest.entity.KSQLRequest;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.RunningQueries;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicsList;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.StatementExecutor;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.SchemaUtil;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.kafka.connect.data.Field;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
  public Response handleKSQLStatements(KSQLRequest request) throws Exception {
    List<Statement> parsedStatements = ksqlEngine.getStatements(request.getKsql());
    List<String> statementStrings = getStatementStrings(request.getKsql());
    if (parsedStatements.size() != statementStrings.size()) {
      throw new Exception(String.format(
          "Size of parsed statements and statement strings differ; %d vs. %d, respectively",
          parsedStatements.size(),
          statementStrings.size()
      ));
    }
    KSQLEntityList result = new KSQLEntityList();
    for (int i = 0; i < parsedStatements.size(); i++) {
      String statementText = statementStrings.get(i);
      try {
        result.add(executeStatement(statementText, parsedStatements.get(i)));
      } catch (Exception exception) {
        result.add(new ErrorMessageEntity(statementText, exception));
      }
    }
    return Response.ok(result).build();
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

  private KSQLEntity executeStatement(String statementText, Statement statement) throws Exception {
    if (statement instanceof ListTopics) {
      return listTopics(statementText);
    } else if (statement instanceof ListStreams) {
      return listStreams(statementText);
    } else if (statement instanceof ListTables) {
      return listTables(statementText);
    } else if (statement instanceof ListQueries) {
      return showQueries(statementText);
    } else if (statement instanceof ShowColumns) {
      return describe(statementText, ((ShowColumns) statement).getTable().getSuffix());
    } else if (statement instanceof SetProperty) {
      return setProperty(statementText, (SetProperty) statement);
    } else if (statement instanceof ListProperties) {
      return listProperties(statementText);
    } else if (statement instanceof CreateTopic
            || statement instanceof CreateStream
            || statement instanceof CreateTable
            || statement instanceof CreateStreamAsSelect
            || statement instanceof CreateTableAsSelect
            || statement instanceof TerminateQuery
    ) {
      CommandId commandId = commandStore.distributeStatement(statementText, statement);
      statementExecutor.registerQueuedStatement(commandId);
      return new CommandIdEntity(statementText, commandId);
    } else {
      if (statement != null) {
        throw new Exception(String.format(
            "Cannot handle statement of type '%s'",
            statement.getClass().getSimpleName()
        ));
      } else if (statementText != null) {
        throw new Exception(String.format(
            "Unable to execute statement '%s'",
            statementText
        ));
      } else {
        throw new Exception("Unable to execute statement");
      }
    }
  }

  private TopicsList listTopics(String statementText) {
    return TopicsList.fromKsqlTopics(statementText, ksqlEngine.getMetaStore().getAllKSQLTopics().values());
  }

  // Only shows queries running on the current machine, not across the entire cluster
  private RunningQueries showQueries(String statementText) {
    List<RunningQueries.RunningQuery> runningQueries = new ArrayList<>();
    for (PersistentQueryMetadata persistentQueryMetadata : ksqlEngine.getPersistentQueries().values()) {
      KSQLStructuredDataOutputNode ksqlStructuredDataOutputNode =
          (KSQLStructuredDataOutputNode) persistentQueryMetadata.getOutputNode();

      runningQueries.add(new RunningQueries.RunningQuery(
          persistentQueryMetadata.getStatementString(),
          ksqlStructuredDataOutputNode.getKafkaTopicName(),
          persistentQueryMetadata.getId()
      ));
    }
    return new RunningQueries(statementText, runningQueries);
  }

  private SourceDescription describe(String statementText, String name) throws Exception {
    JsonObjectBuilder result = Json.createObjectBuilder();
    StructuredDataSource dataSource = ksqlEngine.getMetaStore().getSource(name);
    if (dataSource == null) {
      throw new Exception(String.format("Could not find topic '%s' in the metastore", name));
    }
    result.add("key", (dataSource.getKeyField() != null) ? dataSource.getKeyField().name() :
                      "null");
    result.add("timestamp", (dataSource.getTimestampField() != null) ? dataSource.getTimestampField().name() :
                      "null");
    result.add("type", dataSource.getDataSourceType().toString());
    JsonObjectBuilder fields = Json.createObjectBuilder();
    for (Field schemaField : dataSource.getSchema().fields()) {
      String fieldName = schemaField.name();
      String type = SchemaUtil.TYPE_MAP.get(schemaField.schema().type().getName().toUpperCase()).toUpperCase();
      fields.add(fieldName, type);

    }
    return new SourceDescription(statementText, dataSource);
  }

  // TODO: Right now properties can only be set for a single node. Do we want to distribute this?
  private io.confluent.ksql.rest.entity.SetProperty setProperty(String statementText, SetProperty setProperty) {
    String property = setProperty.getPropertyName();
    Object newValue = setProperty.getPropertyValue();
    Object oldValue = ksqlEngine.setStreamsProperty(property, newValue);
    return new io.confluent.ksql.rest.entity.SetProperty(statementText, property, oldValue, newValue);
  }

  private PropertiesList listProperties(String statementText) {
    return new PropertiesList(statementText, ksqlEngine.getStreamsProperties());
  }

  private StreamsList listStreams(String statementText) {
    return StreamsList.fromKsqlStreams(statementText, getSpecificSources(KSQLStream.class));
  }

  private TablesList listTables(String statementText) {
    return TablesList.fromKsqlTables(statementText, getSpecificSources(KSQLTable.class));
  }

  private <S extends StructuredDataSource> List<S> getSpecificSources(Class<S> dataSourceClass) {
    return ksqlEngine.getMetaStore().getAllStructuredDataSources().values().stream()
        .filter(dataSourceClass::isInstance)
        .map(dataSourceClass::cast)
        .collect(Collectors.toList());
  }
}
