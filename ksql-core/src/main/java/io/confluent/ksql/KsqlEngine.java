/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.DdlEngine;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.CreateTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class KsqlEngine implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(KsqlEngine.class);

  // TODO: Decide if any other properties belong in here
  private static final Set<String> IMMUTABLE_PROPERTIES = new HashSet<>(Arrays.asList(
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
  ));

  private final QueryEngine queryEngine;
  private final DdlEngine ddlEngine;
  private final MetaStore metaStore;
  private final Map<Long, PersistentQueryMetadata> persistentQueries;
  private final Set<QueryMetadata> liveQueries;

  private KsqlConfig ksqlConfig;

  public List<QueryMetadata> buildMultipleQueries(boolean createNewAppId, String queriesString)
      throws Exception {
    return buildMultipleQueries(createNewAppId, queriesString, Collections.emptyMap());
  }

  /**
   * Runs the set of queries in the given query string.
   *
   * @param createNewAppId If a new application id should be generated.
   * @param queriesString The ksql query string.
   * @return List of query metadata.
   * @throws Exception Any exception thrown here!
   */
  public List<QueryMetadata> buildMultipleQueries(
      boolean createNewAppId,
      String queriesString,
      Map<String, Object> overriddenProperties
  ) throws Exception {
    for (String property : overriddenProperties.keySet()) {
      if (IMMUTABLE_PROPERTIES.contains(property)) {
        throw new IllegalArgumentException(
            String.format("Cannot override property '%s'", property)
        );
      }
    }

    // Build query AST from the query string
    List<Pair<String, Query>> queryList = buildQueryAstList(queriesString, overriddenProperties);

    // Logical plan creation from the ASTs
    List<Pair<String, PlanNode>> logicalPlans = queryEngine.buildLogicalPlans(metaStore, queryList);

    // Physical plan creation from logical plans.
    List<QueryMetadata> runningQueries = queryEngine.buildPhysicalPlans(
        createNewAppId,
        metaStore,
        logicalPlans,
        ksqlConfig,
        overriddenProperties,
        true
    );

    for (QueryMetadata queryMetadata : runningQueries) {

      liveQueries.add(queryMetadata);

      if (queryMetadata instanceof PersistentQueryMetadata) {
        PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
        persistentQueries.put(persistentQueryMetadata.getId(), persistentQueryMetadata);
      }
    }

    return runningQueries;
  }

  public String getQueryExecutionPlan(Query query) throws Exception {

    // Logical plan creation from the ASTs
    List<Pair<String, PlanNode>> logicalPlans = queryEngine.buildLogicalPlans(metaStore, Arrays
        .asList(new Pair<>("", query)));

    // Physical plan creation from logical plans.
    List<QueryMetadata> runningQueries = queryEngine.buildPhysicalPlans(
        false,
        metaStore,
        logicalPlans,
        ksqlConfig,
        Collections.emptyMap(),
        false
    );
    return runningQueries.get(0).getExecutionPlan();
  }


  private List<Pair<String, Query>> buildQueryAstList(final String queriesString,
                                                      Map<String, Object> overriddenProperties) {

    // Parse and AST creation
    KsqlParser ksqlParser = new KsqlParser();
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
      Pair<String, Query> queryPair =
          buildSingleQueryAst(statement, getStatementString(singleStatementContext),
                                                          tempMetaStore, overriddenProperties);
      if (queryPair != null) {
        queryList.add(queryPair);
      }
    }
    return queryList;
  }

  private Pair<String, Query> buildSingleQueryAst(Statement statement, String
      statementString, MetaStore tempMetaStore, Map<String, Object> overriddenProperties) {

    log.info("Building AST for {}.", statementString);

    if (statement instanceof Query) {
      return  new Pair<>(statementString, (Query) statement);
    } else if (statement instanceof CreateStreamAsSelect) {
      CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
      QuerySpecification querySpecification =
          (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
      Query query = addInto(
          createStreamAsSelect.getQuery(),
          querySpecification,
          createStreamAsSelect.getName().getSuffix(),
          createStreamAsSelect.getProperties(),
          createStreamAsSelect.getPartitionByColumn()
      );
      tempMetaStore.putSource(queryEngine.getResultDatasource(
          querySpecification.getSelect(),
          createStreamAsSelect.getName().getSuffix()
      ).cloneWithTimeKeyColumns());
      return new Pair<>(statementString, query);
    } else if (statement instanceof CreateTableAsSelect) {
      CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
      QuerySpecification querySpecification =
          (QuerySpecification) createTableAsSelect.getQuery().getQueryBody();

      Query query = addInto(
          createTableAsSelect.getQuery(),
          querySpecification,
          createTableAsSelect.getName().getSuffix(),
          createTableAsSelect.getProperties(),
          Optional.empty()
      );

      tempMetaStore.putSource(queryEngine.getResultDatasource(
          querySpecification.getSelect(),
          createTableAsSelect.getName().getSuffix()
      ).cloneWithTimeKeyColumns());
      return new Pair<>(statementString, query);
    } else if (statement instanceof CreateTopic) {
      KsqlTopic ksqlTopic = ddlEngine.createTopic((CreateTopic) statement, overriddenProperties);
      if (ksqlTopic != null) {
        tempMetaStore.putTopic(ksqlTopic);
      }
    } else if (statement instanceof CreateStream) {
      KsqlStream ksqlStream = ddlEngine.createStream((CreateStream) statement);
      if (ksqlStream != null) {
        tempMetaStore.putSource(ksqlStream);
      }
    } else if (statement instanceof CreateTable) {
      KsqlTable ksqlTable = ddlEngine.createTable((CreateTable) statement);
      if (ksqlTable != null) {
        tempMetaStore.putSource(ksqlTable);
      }
    }
    return null;
  }

  public static String getStatementString(
      SqlBaseParser.SingleStatementContext singleStatementContext) {
    CharStream charStream = singleStatementContext.start.getInputStream();
    return charStream.getText(new Interval(
        singleStatementContext.start.getStartIndex(),
        singleStatementContext.stop.getStopIndex()
    ));
  }

  public List<Statement> getStatements(final String sqlString) {
    return new KsqlParser().buildAst(sqlString, metaStore);
  }


  public Query addInto(final Query query, final QuerySpecification querySpecification,
                       final String intoName,
                       final Map<String,
                           Expression> intoProperties,
                       Optional<Expression> partitionByExpression) {
    Table intoTable = new Table(QualifiedName.of(intoName));
    if (partitionByExpression.isPresent()) {
      Map<String, Expression> newIntoProperties = new HashMap<>();
      newIntoProperties.putAll(intoProperties);
      newIntoProperties.put(DdlConfig.PARTITION_BY_PROPERTY, partitionByExpression.get());
      intoTable.setProperties(newIntoProperties);
    } else {
      intoTable.setProperties(intoProperties);
    }

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

  public DdlEngine getDdlEngine() {
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

  public static List<String> getImmutableProperties() {
    return new ArrayList<>(IMMUTABLE_PROPERTIES);
  }

  public Map<String, Object> getStreamsProperties() {
    return ksqlConfig.getStreamsProperties();
  }

  @Override
  public void close() {
    for (QueryMetadata queryMetadata : liveQueries) {
      queryMetadata.getKafkaStreams().close();
    }
  }

  public KsqlEngine(MetaStore metaStore, Map<String, Object> streamsProperties) {
    this.ksqlConfig = new KsqlConfig(streamsProperties);
    this.metaStore = metaStore;
    this.queryEngine = new QueryEngine(ksqlConfig);
    this.ddlEngine = new DdlEngine(this);
    this.persistentQueries = new HashMap<>();
    this.liveQueries = new HashSet<>();
  }

  public QueryEngine getQueryEngine() {
    return queryEngine;
  }
}
