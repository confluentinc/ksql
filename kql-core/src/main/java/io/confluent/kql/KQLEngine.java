/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql;

import io.confluent.kql.ddl.DDLEngine;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.metastore.MetaStoreImpl;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.parser.KQLParser;
import io.confluent.kql.parser.SqlBaseParser;
import io.confluent.kql.parser.tree.Query;
import io.confluent.kql.parser.tree.QuerySpecification;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.parser.tree.Table;
import io.confluent.kql.parser.tree.CreateStreamAsSelect;
import io.confluent.kql.parser.tree.CreateTableAsSelect;
import io.confluent.kql.parser.tree.SelectItem;
import io.confluent.kql.parser.tree.QualifiedName;
import io.confluent.kql.parser.tree.CreateTopic;
import io.confluent.kql.parser.tree.CreateStream;
import io.confluent.kql.parser.tree.CreateTable;
import io.confluent.kql.parser.tree.SingleColumn;
import io.confluent.kql.parser.tree.Select;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.planner.plan.PlanNode;
import io.confluent.kql.util.DataSourceExtractor;
import io.confluent.kql.util.KQLConfig;
import io.confluent.kql.util.Pair;
import io.confluent.kql.util.PersistentQueryMetadata;
import io.confluent.kql.util.QueryMetadata;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class KQLEngine implements Closeable {

  private final QueryEngine queryEngine;
  private final DDLEngine ddlEngine;
  private final MetaStore metaStore;
  private final Map<Long, PersistentQueryMetadata> persistentQueries;
  private final Set<QueryMetadata> liveQueries;

  /**
   * Runs the set of queries in the given query string. This method is used when the queries are
   * passed through a file.
   *
   * @param queriesString
   * @return
   * @throws Exception
   */
  public List<QueryMetadata> runMultipleQueries(boolean createNewAppId, String queriesString) throws Exception {

    // Parse and AST creation
    KQLParser kqlParser = new KQLParser();
    List<SqlBaseParser.SingleStatementContext>
        parsedStatements =
        kqlParser.getStatements(queriesString);
    List<Pair<String, Query>> queryList = new ArrayList<>();
    MetaStore tempMetaStore = new MetaStoreImpl();
    for (String dataSourceName : metaStore.getAllStructuredDataSourceNames()) {
      tempMetaStore.putSource(metaStore.getSource(dataSourceName));
    }
    for (SqlBaseParser.SingleStatementContext singleStatementContext : parsedStatements) {
      Pair<Statement, DataSourceExtractor> statementInfo =
          kqlParser.prepareStatement(singleStatementContext, tempMetaStore);
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
        KQLTopic kqlTopic = ddlEngine.createTopic((CreateTopic) statement);
        if (kqlTopic != null) {
          tempMetaStore.putTopic(kqlTopic);
        }

      } else if (statement instanceof CreateStream) {
        KQLStream kqlStream = ddlEngine.createStream((CreateStream) statement);
        if (kqlStream != null) {
          tempMetaStore.putSource(kqlStream);
        }
      } else if (statement instanceof CreateTable) {
        KQLTable kqlTable = ddlEngine.createTable((CreateTable) statement);
        if (kqlTable != null) {
          tempMetaStore.putSource(kqlTable);
        }
      }
    }

    // Logical plan creation from the ASTs
    List<Pair<String, PlanNode>> logicalPlans = queryEngine.buildLogicalPlans(metaStore, queryList);

    // Physical plan creation from logical plans.
    List<QueryMetadata> runningQueries = queryEngine.buildPhysicalPlans(createNewAppId, metaStore, logicalPlans);

    for (QueryMetadata queryMetadata : runningQueries) {

      liveQueries.add(queryMetadata);

      if (queryMetadata instanceof PersistentQueryMetadata) {
        PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
        persistentQueries.put(persistentQueryMetadata.getId(), persistentQueryMetadata);
      }
    }

    return runningQueries;
  }

  public StructuredDataSource getResultDatasource(final Select select, final Table into) {

    SchemaBuilder dataSource = SchemaBuilder.struct().name(into.toString());

    for (SelectItem selectItem : select.getSelectItems()) {
      if (selectItem instanceof SingleColumn) {
        SingleColumn singleColumn = (SingleColumn) selectItem;
        String fieldName = singleColumn.getAlias().get();
        dataSource = dataSource.field(fieldName, Schema.BOOLEAN_SCHEMA);
      }
    }

    KQLTopic kqlTopic = new KQLTopic(into.getName().toString(), into.getName().toString(), null);
    return new KQLStream(into.getName().toString(), dataSource.schema(), dataSource.fields().get(0), kqlTopic);
  }

  private String getStatementString(SqlBaseParser.SingleStatementContext singleStatementContext) {
    CharStream charStream = singleStatementContext.start.getInputStream();
    return charStream.getText(new Interval(
        singleStatementContext.start.getStartIndex(),
        singleStatementContext.stop.getStopIndex()
    ));
  }

  public List<Statement> getStatements(final String sqlString) {
    return new KQLParser().buildAST(sqlString, metaStore);
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

  @Override
  public void close() {
    for (QueryMetadata queryMetadata : liveQueries) {
      queryMetadata.getKafkaStreams().close();
    }
  }

  public KQLEngine(MetaStore metaStore, KQLConfig kqlConfig) {
    this.metaStore = metaStore;
    this.queryEngine = new QueryEngine(kqlConfig);
    this.ddlEngine = new DDLEngine(this);
    this.persistentQueries = new HashMap<>();
    this.liveQueries = new HashSet<>();
  }

  public QueryEngine getQueryEngine() {
    return queryEngine;
  }
}
