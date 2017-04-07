package io.confluent.kql.rest.computation;

import io.confluent.kql.KQLEngine;
import io.confluent.kql.ddl.DDLConfig;
import io.confluent.kql.parser.tree.CreateStream;
import io.confluent.kql.parser.tree.CreateStreamAsSelect;
import io.confluent.kql.parser.tree.CreateTable;
import io.confluent.kql.parser.tree.CreateTableAsSelect;
import io.confluent.kql.parser.tree.CreateTopic;
import io.confluent.kql.parser.tree.Query;
import io.confluent.kql.parser.tree.QuerySpecification;
import io.confluent.kql.parser.tree.Relation;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.parser.tree.Table;
import io.confluent.kql.parser.tree.TerminateQuery;
import io.confluent.kql.planner.plan.KQLStructuredDataOutputNode;
import io.confluent.kql.rest.StatementParser;
import io.confluent.kql.rest.TopicUtil;
import io.confluent.kql.util.KQLException;
import io.confluent.kql.util.QueryMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class QueryHandler {

  private static final Logger log = LoggerFactory.getLogger(QueryHandler.class);

  private final TopicUtil topicUtil;
  private final KQLEngine kqlEngine;
  private final Map<String, QueryMetadata> liveQueryMap;
  private final StatementParser statementParser;
  private final Map<String, StatementStatus> statusStore;

  public QueryHandler(
      TopicUtil topicUtil,
      KQLEngine kqlEngine,
      Map<String, QueryMetadata> liveQueryMap,
      StatementParser statementParser,
      Map<String, StatementStatus> statusStore
  ) {
    this.topicUtil = topicUtil;
    this.kqlEngine = kqlEngine;
    this.liveQueryMap = liveQueryMap;
    this.statementParser = statementParser;
    this.statusStore = statusStore;
  }

  public void handleStatement(String statementString, String statementId) throws Exception {
    Statement statement = statementParser.parseSingleStatement(statementString);
    statusStore.put(statementId, new StatementStatus(StatementStatus.Status.EXECUTING, "Executing statement"));
    handleStatement(statement, statementString, statementId);
  }

  // Copied from the io.confluent.kql.ddl.DDLEngine
  private String enforceString(final String propertyName, final String propertyValue) {
    if (!propertyValue.startsWith("'") && !propertyValue.endsWith("'")) {
      throw new KQLException(propertyName + " value is string and should be enclosed between "
          + "\"'\".");
    }
    return propertyValue.substring(1, propertyValue.length() - 1);
  }

  private void handleStatement(Statement statement, String statementStr, String statementId) throws Exception {
    if (statement instanceof CreateTopic) {
      CreateTopic createTopic = (CreateTopic) statement;
      String kafkaTopicName =
          createTopic.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY).toString();
      kafkaTopicName = enforceString(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY, kafkaTopicName);
      topicUtil.ensureTopicExists(kafkaTopicName);
      kqlEngine.getDdlEngine().createTopic(createTopic);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.SUCCESS, "Topic created"));
    } else if (statement instanceof CreateStream) {
      CreateStream createStream = (CreateStream) statement;
      String streamName = createStream.getName().getSuffix();
      topicUtil.ensureTopicExists(streamName);
      kqlEngine.getDdlEngine().createStream(createStream);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.SUCCESS, "Stream created"));
    } else if (statement instanceof CreateTable) {
      CreateTable createTable = (CreateTable) statement;
      String tableName = createTable.getName().getSuffix();
      topicUtil.ensureTopicExists(tableName);
      kqlEngine.getDdlEngine().createTable(createTable);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.SUCCESS, "Table created"));
    } else if (statement instanceof CreateStreamAsSelect) {
      CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
      QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
      Query query = kqlEngine.addInto(
          createStreamAsSelect.getQuery(),
          querySpecification,
          createStreamAsSelect.getName().getSuffix(),
          createStreamAsSelect.getProperties()
      );
      String streamName = createStreamAsSelect.getName().getSuffix();
      topicUtil.ensureTopicExists(streamName);
      String queryId = statementId + "_QUERY";
      startQuery(statementStr, query, queryId);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.RUNNING, "Stream created and running"));
    } else if (statement instanceof CreateTableAsSelect) {
      CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
      QuerySpecification querySpecification = (QuerySpecification) createTableAsSelect.getQuery().getQueryBody();
      Query query = kqlEngine.addInto(
          createTableAsSelect.getQuery(),
          querySpecification,
          createTableAsSelect.getName().getSuffix(),
          createTableAsSelect.getProperties()
      );
      String tableName = createTableAsSelect.getName().getSuffix();
      topicUtil.ensureTopicExists(tableName);
      String queryId = statementId + "_QUERY";
      startQuery(statementStr, query, queryId);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.RUNNING, "Table created and running"));
    } else if (statement instanceof TerminateQuery) {
      terminateQuery((TerminateQuery) statement);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.SUCCESS, "Termination request granted"));
    } else {
      throw new Exception(String.format(
          "Unexpected statement type: %s",
          statement.getClass().getName()
      ));
    }
  }

  private void startQuery(String queryString, Query query, String queryId) throws Exception {
    if (query.getQueryBody() instanceof QuerySpecification) {
      QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
      Optional<Relation> into = querySpecification.getInto();
      if (into.isPresent() && into.get() instanceof Table) {
        Table table = (Table) into.get();
        if (kqlEngine.getMetaStore().getSource(table.getName().getSuffix()) != null) {
          throw new Exception(String.format(
              "Sink specified in INTO clause already exists: %s",
              table.getName().getSuffix().toUpperCase()
          ));
        }
      }
    }

    QueryMetadata queryPairInfo = kqlEngine.runMultipleQueries(false, queryString).get(0);

    if (queryPairInfo.getQueryOutputNode() instanceof KQLStructuredDataOutputNode) {
      // TODO: The query string shouldn't really be used as the query ID in the metadata here... think about a better way to store live queries
      QueryMetadata queryInfo =
          new QueryMetadata(queryString, queryPairInfo.getQueryKafkaStreams(), queryPairInfo.getQueryOutputNode());
      liveQueryMap.put(queryId, queryInfo);
    } else {
      throw new Exception(String.format("Unexpected query output node: %s", queryPairInfo.getQueryOutputNode()));
    }
  }

  private void terminateQuery(TerminateQuery terminateQuery) throws Exception {
    String queryId = terminateQuery.getQueryId().toString();
    if (!liveQueryMap.containsKey(queryId)) {
      throw new Exception(String.format("No running query with id '%s' was found", queryId));
    }
    QueryMetadata kafkaStreamsQuery = liveQueryMap.get(queryId);
    kafkaStreamsQuery.getQueryKafkaStreams().close();
    liveQueryMap.remove(queryId);
    String queryStatementId = queryId.substring(0, queryId.length() - "_QUERY".length());
    statusStore.put(queryStatementId, new StatementStatus(StatementStatus.Status.TERMINATED, "Query terminated"));
  }
}
