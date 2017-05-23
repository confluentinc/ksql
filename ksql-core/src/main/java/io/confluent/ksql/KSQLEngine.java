/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql;

import io.confluent.ksql.ddl.DDLEngine;
import io.confluent.ksql.metastore.KSQLStream;
import io.confluent.ksql.metastore.KSQLTable;
import io.confluent.ksql.metastore.KSQLTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.CreateTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.KSQLConfig;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.StreamsConfig;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class KSQLEngine implements Closeable {

  private final QueryEngine queryEngine;
  private final DDLEngine ddlEngine;
  private final MetaStore metaStore;
  private final Map<Long, PersistentQueryMetadata> persistentQueries;
  private final Set<QueryMetadata> liveQueries;

  private KSQLConfig ksqlConfig;

  /**
   * Runs the set of queries in the given query string.
   *
   * @param queriesString
   * @return
   * @throws Exception
   */
  public List<QueryMetadata> buildMultipleQueries(boolean createNewAppId, String queriesString) throws Exception {

    // Parse and AST creation
    KSQLParser ksqlParser = new KSQLParser();
    List<SqlBaseParser.SingleStatementContext>
        parsedStatements =
        ksqlParser.getStatements(queriesString);
    List<Pair<String, Query>> queryList = new ArrayList<>();
    MetaStore tempMetaStore = new MetaStoreImpl();
    for (String dataSourceName : metaStore.getAllStructuredDataSourceNames()) {
      tempMetaStore.putSource(metaStore.getSource(dataSourceName));
    }
    for (SqlBaseParser.SingleStatementContext singleStatementContext : parsedStatements) {
      Pair<Statement, DataSourceExtractor> statementInfo =
          ksqlParser.prepareStatement(singleStatementContext, tempMetaStore);
      Statement statement = statementInfo.getLeft();
      if (statement instanceof Query) {
        queryList.add(new Pair<>(getStatementString(singleStatementContext), (Query) statement));
      } else if (statement instanceof CreateStreamAsSelect) {
        CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
        QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
        Query query = addInto(
            createStreamAsSelect.getQuery(),
            querySpecification,
            createStreamAsSelect.getName().getSuffix(),
            createStreamAsSelect.getProperties()
        );
        tempMetaStore.putSource(queryEngine.getResultDatasource(
            querySpecification.getSelect(),
            createStreamAsSelect.getName().getSuffix()
        ));
        queryList.add(new Pair<>(getStatementString(singleStatementContext), query));
      } else if (statement instanceof CreateTableAsSelect) {
        CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
        QuerySpecification querySpecification = (QuerySpecification) createTableAsSelect.getQuery().getQueryBody();

        Query query = addInto(
            createTableAsSelect.getQuery(),
            querySpecification,
            createTableAsSelect.getName().getSuffix(),
            createTableAsSelect.getProperties()
        );

        tempMetaStore.putSource(queryEngine.getResultDatasource(
            querySpecification.getSelect(),
            createTableAsSelect.getName().getSuffix()
        ));
        queryList.add(new Pair<>(getStatementString(singleStatementContext), query));
      } else if (statement instanceof CreateTopic) {
        KSQLTopic ksqlTopic = ddlEngine.createTopic((CreateTopic) statement);
        if (ksqlTopic != null) {
          tempMetaStore.putTopic(ksqlTopic);
        }

      } else if (statement instanceof CreateStream) {
        KSQLStream ksqlStream = ddlEngine.createStream((CreateStream) statement);
        if (ksqlStream != null) {
          tempMetaStore.putSource(ksqlStream);
        }
      } else if (statement instanceof CreateTable) {
        KSQLTable ksqlTable = ddlEngine.createTable((CreateTable) statement);
        if (ksqlTable != null) {
          tempMetaStore.putSource(ksqlTable);
        }
      }
    }

    // Logical plan creation from the ASTs
    List<Pair<String, PlanNode>> logicalPlans = queryEngine.buildLogicalPlans(metaStore, queryList);

    // Physical plan creation from logical plans.
    List<QueryMetadata> runningQueries = queryEngine.buildPhysicalPlans(createNewAppId,
                                                                        metaStore, logicalPlans,
                                                                        ksqlConfig);

    for (QueryMetadata queryMetadata : runningQueries) {

      liveQueries.add(queryMetadata);

      if (queryMetadata instanceof PersistentQueryMetadata) {
        PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
        persistentQueries.put(persistentQueryMetadata.getId(), persistentQueryMetadata);
      }
    }

    return runningQueries;
  }

  public static String getStatementString(SqlBaseParser.SingleStatementContext singleStatementContext) {
    CharStream charStream = singleStatementContext.start.getInputStream();
    return charStream.getText(new Interval(
        singleStatementContext.start.getStartIndex(),
        singleStatementContext.stop.getStopIndex()
    ));
  }

  public List<Statement> getStatements(final String sqlString) {
    return new KSQLParser().buildAST(sqlString, metaStore);
  }


  public Query addInto(final Query query, final QuerySpecification querySpecification,
                       final String intoName,
                       final Map<String,
                           Expression> intoProperties) {
    Table intoTable = new Table(QualifiedName.of(intoName));
    intoTable.setProperties(intoProperties);
    QuerySpecification newQuerySpecification = new QuerySpecification(
        querySpecification.getSelect(),
        Optional.of(intoTable),
        querySpecification.getFrom(),
        querySpecification.getWindowExpression(),
        querySpecification.getWhere(),
        querySpecification.getGroupBy(),
        querySpecification.getHaving(),
        querySpecification.getOrderBy(),
        querySpecification.getLimit()
    );
    return new Query(query.getWith(), newQuerySpecification, query.getOrderBy(), query.getLimit());
  }

  public MetaStore getMetaStore() {
    return metaStore;
  }

  public DDLEngine getDdlEngine() {
    return ddlEngine;
  }

  public boolean terminateQuery(long queryId, boolean closeStreams) {
    QueryMetadata queryMetadata = persistentQueries.remove(queryId);
    if (queryMetadata == null) {
      return false;
    }
    liveQueries.remove(queryMetadata);
    if (closeStreams) {
      queryMetadata.getKafkaStreams().close();
    }
    return true;
  }

  public Map<Long, PersistentQueryMetadata> getPersistentQueries() {
    return new HashMap<>(persistentQueries);
  }

  public Set<QueryMetadata> getLiveQueries() {
    return new HashSet<>(liveQueries);
  }

  public static boolean isValidStreamsProperty(String property) {
    if (property.startsWith(StreamsConfig.CONSUMER_PREFIX)) {
      String strippedProperty = property.substring(StreamsConfig.CONSUMER_PREFIX.length());
      return ConsumerConfig.configNames().contains(strippedProperty);
    } else if (property.startsWith(StreamsConfig.PRODUCER_PREFIX)) {
      String strippedProperty = property.substring(StreamsConfig.PRODUCER_PREFIX.length());
      return ProducerConfig.configNames().contains(strippedProperty);
    } else {
      return StreamsConfig.configDef().names().contains(property)
          || ConsumerConfig.configNames().contains(property)
          || ProducerConfig.configNames().contains(property);
    }
  }

  // May want to flesh this out a little more in the future and improve error messages, but right now this does what it
  // needs to--makes sure the given properties would create a valid StreamsConfig object
  public void validateStreamsProperties(Map<String, Object> streamsProperties) {
    new StreamsConfig(streamsProperties);
    // TODO: Validate consumer and producer properties as well
  }

  public void setStreamsProperty(String property, Object value) {
    if (!isValidStreamsProperty(property)) {
      throw new IllegalArgumentException(String.format("'%s' is not a valid property", property));
    }

    Map<String, Object> newProperties = ksqlConfig.getKsqlConfigProps();
    newProperties.put(property, value);

    try {
      validateStreamsProperties(newProperties);
    } catch (ConfigException configException) {
      throw new IllegalArgumentException(String.format("Invalid value for '%s' property: '%s'", property, value));
    }
    ksqlConfig.put(property, value);
  }

  public Map<String, Object> getStreamsProperties() {
    Map<String, Object> result = new HashMap<>();
    for (Map.Entry<String, Object> propertyEntry : ksqlConfig.getKsqlConfigProps().entrySet()) {
      if (isValidStreamsProperty(propertyEntry.getKey())) {
        result.put(propertyEntry.getKey(), propertyEntry.getValue());
      }
    }
    return result;
  }

  @Override
  public void close() {
    for (QueryMetadata queryMetadata : liveQueries) {
      queryMetadata.getKafkaStreams().close();
    }
  }

  public KSQLEngine(MetaStore metaStore, Map<String, Object> streamsProperties) {
    validateStreamsProperties(streamsProperties);

    this.ksqlConfig = new KSQLConfig(streamsProperties);
    this.metaStore = metaStore;
    this.queryEngine = new QueryEngine(ksqlConfig);
    this.ddlEngine = new DDLEngine(this);
    this.persistentQueries = new HashMap<>();
    this.liveQueries = new HashSet<>();
  }

  public QueryEngine getQueryEngine() {
    return queryEngine;
  }
}
