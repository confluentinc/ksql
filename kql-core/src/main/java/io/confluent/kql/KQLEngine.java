package io.confluent.kql;


import io.confluent.kql.ddl.DDLEngine;
import io.confluent.kql.metastore.*;
import io.confluent.kql.parser.KQLParser;
import io.confluent.kql.parser.KQLParser;
import io.confluent.kql.parser.SqlBaseParser;
import io.confluent.kql.parser.tree.*;
import io.confluent.kql.planner.plan.OutputNode;
import io.confluent.kql.planner.plan.PlanNode;
import io.confluent.kql.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class KQLEngine {

  KQLConfig kqlConfig;
  QueryEngine queryEngine;
  DDLEngine ddlEngine = new DDLEngine(this);
  MetaStore metaStore = null;

  public List<Triplet<String, KafkaStreams, OutputNode>> runMultipleQueries(
      String queriesString) throws Exception {

    // Parse and AST creation
    KQLParser kqlParser = new KQLParser();
    List<SqlBaseParser.SingleStatementContext>
        parsedStatements =
        kqlParser.getStatements(queriesString);
    int queryIndex = 0;
    List<Pair<String, Query>> queryList = new ArrayList<>();
    MetaStore tempMetaStore = new MetaStoreImpl();
    for (String dataSourceName : metaStore.getAllStructuredDataSource().keySet()) {
      tempMetaStore.putSource(metaStore.getSource(dataSourceName));
    }
    for (SqlBaseParser.SingleStatementContext singleStatementContext : parsedStatements) {
      Pair<Statement, DataSourceExtractor>
          statementInfo =
          kqlParser.prepareStatement(singleStatementContext, tempMetaStore);
      Statement statement = statementInfo.getLeft();
      if (statement instanceof Query) {
        Query query = (Query) statement;
        QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
        Table intoTable = (Table)querySpecification.getInto().get();
        tempMetaStore.putSource(queryEngine.getResultDatasource(querySpecification.getSelect(),
                                                                intoTable.getName().getSuffix()
                                                                    ));
        queryList.add(new Pair("KQL_QUERY_" + queryIndex, query));
        queryIndex++;
      } else if (statement instanceof CreateStreamAsSelect) {
        CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect)statement;
        QuerySpecification querySpecification = (QuerySpecification)createStreamAsSelect.getQuery()
            .getQueryBody();
        Query query = addInto(createStreamAsSelect.getQuery(), querySpecification,
                                         createStreamAsSelect.getName().getSuffix(),createStreamAsSelect
                                             .getProperties());
        tempMetaStore.putSource(queryEngine.getResultDatasource(querySpecification.getSelect(), createStreamAsSelect.getName().getSuffix()));
        queryList.add(new Pair("KQL_QUERY_" + queryIndex, query));
        queryIndex++;
      } else if (statement instanceof CreateTableAsSelect) {
        CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
        QuerySpecification querySpecification = (QuerySpecification)createTableAsSelect.getQuery()
            .getQueryBody();

        Query query = addInto(createTableAsSelect.getQuery(), querySpecification,
                                         createTableAsSelect.getName().getSuffix(),createTableAsSelect
                                             .getProperties());

        tempMetaStore.putSource(queryEngine.getResultDatasource(querySpecification.getSelect(), createTableAsSelect.getName().getSuffix()));
        queryList.add(new Pair("KQL_QUERY_" + queryIndex, query));
        queryIndex++;
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
    List<Triplet<String, KafkaStreams, OutputNode>>
        runningQueries =
        queryEngine.buildRunPhysicalPlans(false, metaStore, logicalPlans);

    return runningQueries;

  }

  public StructuredDataSource getResultDatasource(Select select, Table into) {

    SchemaBuilder dataSource = SchemaBuilder.struct().name(into.toString());

    for (SelectItem selectItem : select.getSelectItems()) {
      if (selectItem instanceof SingleColumn) {
        SingleColumn singleColumn = (SingleColumn) selectItem;
        String fieldName = singleColumn.getAlias().get();
        String fieldType = null;
        dataSource = dataSource.field(fieldName, Schema.BOOLEAN_SCHEMA);
      }


    }

    KQLTopic KQLTopic = new KQLTopic(into.getName().toString(), into.getName().toString(),
                                     null);
    StructuredDataSource
        resultStream =
        new KQLStream(into.getName().toString(), dataSource.schema(), dataSource.fields().get(0),
                      KQLTopic
        );
    return resultStream;
  }

  public void runCLIQuery(String queriyString, long terminateIn) throws Exception {
    // Parse and AST creation
    KQLParser kqlParser = new KQLParser();
    List<SqlBaseParser.SingleStatementContext>
        parsedStatements =
        kqlParser.getStatements(queriyString);
    int queryIndex = 0;
    List<Pair<String, Query>> queryList = new ArrayList<>();
    MetaStore tempMetaStore = new MetaStoreImpl();
    for (String dataSourceName : metaStore.getAllStructuredDataSource().keySet()) {
      tempMetaStore.putSource(metaStore.getSource(dataSourceName));
    }
    for (SqlBaseParser.SingleStatementContext singleStatementContext : parsedStatements) {
      Pair<Statement, DataSourceExtractor>
          statementInfo =
          kqlParser.prepareStatement(singleStatementContext, tempMetaStore);
      Statement statement = statementInfo.getLeft();
      if (statement instanceof Query) {
        Query query = (Query) statement;
        queryList.add(new Pair("KQL_QUERY_" + queryIndex, query));
        queryIndex++;
      } else if (statement instanceof CreateTopic) {
        ddlEngine.createTopic((CreateTopic) statement);
      } else if (statement instanceof DropTable) {
        ddlEngine.dropTopic((DropTable) statement);
      }
    }

    queryEngine.buildRunSingleConsoleQuery(metaStore, queryList, terminateIn);
  }

  public Triplet<String, KafkaStreams, OutputNode> runSingleQuery(
      Pair<String, Query> queryInfo) throws Exception {

    List<Pair<String, PlanNode>>
        logicalPlans =
        queryEngine.buildLogicalPlans(metaStore, Arrays.asList(queryInfo));

    // Physical plan creation from logical plans.
    List<Triplet<String, KafkaStreams, OutputNode>>
        runningQueries =
        queryEngine.buildRunPhysicalPlans(true, metaStore, logicalPlans);
    return runningQueries.get(0);

  }

  public List<Statement> getStatements(String sqlString) {
    // First parse the query and build the AST
    KQLParser kqlParser = new KQLParser();
    List<Statement> builtASTStatements = kqlParser.buildAST(sqlString, metaStore);
    return builtASTStatements;
  }


  public void processStatements(String queryId, String statementsString) throws Exception {
    if (!statementsString.endsWith(";")) {
      statementsString = statementsString + ";";
    }
    // Parse the query and build the AST
    List<Statement> statements = getStatements(statementsString);
    int internalIndex = 0;
    for (Statement statement : statements) {
      if (statement instanceof Query) {
        queryEngine.processQuery(queryId + "_" + internalIndex, (Query) statement, metaStore);
        internalIndex++;
      } else if (statement instanceof CreateTable) {
        ddlEngine.createTopic((CreateTopic) statement);
        return;
      } else if (statement instanceof DropTable) {
        ddlEngine.dropTopic((DropTable) statement);
        return;
      }
    }
  }

  public Query addInto(Query query, QuerySpecification querySpecification, String intoName,
                        Map<String,
                            Expression> intoProperties) {
    Table intoTable = new Table(QualifiedName.of(intoName));
    intoTable.setProperties(intoProperties);
    QuerySpecification newQuerySpecification = new QuerySpecification(querySpecification
                                                                          .getSelect(),
                                                                      java.util.Optional
                                                                          .ofNullable(intoTable),
                                                                      querySpecification.getFrom
                                                                          (), querySpecification
                                                                          .getWhere(),
                                                                      querySpecification
                                                                          .getGroupBy(),
                                                                      querySpecification
                                                                          .getHaving(),
                                                                      querySpecification
                                                                          .getOrderBy(),
                                                                      querySpecification.getLimit
                                                                          ());
    return new Query(query.getWith(), newQuerySpecification, query.getOrderBy(), query.getLimit()
        , query.getApproximate());
  }

  public MetaStore getMetaStore() {
    return metaStore;
  }

  public QueryEngine getQueryEngine() {
    return queryEngine;
  }

  public DDLEngine getDdlEngine() {
    return ddlEngine;
  }

  public KQLConfig getKqlConfig() {
    return kqlConfig;
  }

  public KQLEngine(String schemaFilePath) throws IOException {
    this.metaStore = new MetastoreUtil().loadMetastoreFromJSONFile(schemaFilePath);
  }

  public KQLEngine(Map<String, String> kqlConfProperties) throws IOException {
    String schemaPath = kqlConfProperties.get(KQLConfig.CATALOG_FILE_PATH_CONFIG);
    Properties kqlProperties = new Properties();
    kqlProperties
        .put(StreamsConfig.APPLICATION_ID_CONFIG, "KQL-Default-" + System.currentTimeMillis());
    kqlProperties
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KQLConfig.DEFAULT_BOOTSTRAP_SERVERS_CONFIG);
    kqlProperties
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KQLConfig.DEFAULT_AUTO_OFFSET_RESET_CONFIG);
    if (!kqlConfProperties.get(KQLConfig.PROP_FILE_PATH_CONFIG)
        .equalsIgnoreCase(KQLConfig.DEFAULT_PROP_FILE_PATH_CONFIG)) {
      kqlProperties.load(new FileReader(kqlConfProperties.get(KQLConfig.PROP_FILE_PATH_CONFIG)));
    }

    this.kqlConfig = new KQLConfig(kqlProperties);
    this.metaStore = new MetastoreUtil().loadMetastoreFromJSONFile(schemaPath);
    this.queryEngine = new QueryEngine(this.kqlConfig);
  }

  public static void main(String[] args) throws Exception {
    Map<String, String> kqlConfProperties = new HashMap<>();
//    kqlConfProperties.put(KQLConfig.SCHEMA_FILE_PATH_CONFIG, "/Users/hojjat/userschema.json");
    kqlConfProperties.put(KQLConfig.CATALOG_FILE_PATH_CONFIG, "/tmp/order_catalog.json");
//    kqlConfProperties.put(KQLConfig.PROP_FILE_PATH_CONFIG, "/Users/hojjat/kql_config.conf");
    KQLEngine kqlEngine = new KQLEngine(kqlConfProperties);

    kqlEngine.processStatements("KQL_1","SELECT *  FROM orders ;");

//    kqlEngine.runCLIQuery("SELECT ordertime AS timeValue, orderid , orderunits%10, lower(itemid)"
//                           + "  "
//                           + "FROM orders;"
//                           + "", -1);

//    kqlEngine.processStatements("KQL_1","SELECT ordertime AS timeValue, orderid , "
//                                          + "orderunits%10, lcase(itemid) FROM orders");

//    kqlEngine.processStatements("KQL_1","SELECT lcase(itemid) FROM orders");

//    kqlEngine.processStatements("KQL_1","SELECT len(orderid), CAST(substring(itemid,5)  AS "
//                                          + "INTEGER) FROM orders "
//                                          + "WHERE "
//                                          + "itemid "
//                                          + "LIKE "
//                                          + "'%5'");
//    kqlEngine.processStatements("KQL_1","SELECT ordertime AS timeValue, orderid , orderunits%10 into stream5 FROM orders WHERE NOT (orderunits > 5) ;");
//    kqlEngine.processStatements("KQL_1","SELECT ordertime AS timeValue, orderid , orderunits%10"
//                                          + " into stream1 FROM orders ;");
//    kqlEngine.processStatements("KQL_1","SELECT USERID, REGIONID "
//                                          + "  FROM users where userid = 'User_65';");
//    kqlEngine.processStatements("KQL_1","SELECT USERID, PAGEID "
//                                          + "  FROM pageview where userid = 'User_65';");

//    kqlEngine.processStatements("KQL_1","SELECT * "
//                                          + "  FROM users ;");
//    kqlEngine.processStatements("KQL_1","SELECT * "
//                                          + "  FROM orders ;");

//    kqlEngine.processStatements("KQL_1", "select pageview.USERID, users.USERID, PAGEID, REGIONID, VIEWTIME FROM pageview LEFT JOIN users ON pageview.USERID = users.USERID;");

//        kqlEngine.processStatements("CREATE TOPIC orders ( orderkey bigint, orderstatus varchar, totalprice double, orderdate date)".toUpperCase());
//        kqlEngine.processStatements("KQL_1","SELECT ordertime AS timeValue, orderid , orderunits%10 into stream5 FROM orders WHERE NOT (orderunits > 5) ;");
//        kqlEngine.processStatements("KQL_1","SELECT ordertime AS timeValue, orderid , orderunits%10 into stream5 FROM orders WHERE orderid IS NOT NULL ;");
//        kqlEngine.processStatements("KQL_1", "select o.ordertime+1, o.itemId, orderunits into stream1 from orders o where o.orderunits > 5;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select * into stream1 from orders JOIN shipment ON orderid = shipmentorderid where orderunits > 5;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select u.userid, p.pageid , p.viewtime, regionid into stream3 from  pageview p LEFT JOIN users u ON u.userid = p.userid;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select u.userid, p.userid, p.pageid , p.viewtime, regionid into stream1 from  pageview p JOIN users u ON u.userid = p.userid;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select pageview.USERID, users.USERID, PAGEID, REGIONID, VIEWTIME into stream2 FROM pageview JOIN users ON pageview.USERID = users.USERID;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select USERID, REGIONID into stream8 from users where REGIONID = 'Region_5';".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select USERID, REGIONID, userid = '*****', 12 into stream11 from users;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select * into ktable1 from users;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select * from orders;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select pageview.USERID, users.USERID, PAGEID, REGIONID, VIEWTIME into stream6 FROM pageview LEFT JOIN users ON pageview.USERID = users.USERID;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select ORDERTIME, ITEMID, ORDERUNITS into stream6 from orders where ORDERUNITS > 8 AND ITEMID = 'Item_3';".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select users.userid, pageid, regionid, gender into stream3 from pageview left join users on pageview.userid = users.userid;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "SELECT PAGEVIEW.USERID, PAGEID, REGIONID, GENDER INTO PAGEVIEWJOIN1 FROM PAGEVIEW LEFT JOIN USERS ON PAGEVIEW.USERID = USERS.USERID;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select USERTIME, USERID, REGIONID into stream5 from users;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "select ordertime, itemId, orderunits, '**===*' AS t into stream3 from orders;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "SELECT users.userid AS userid, pageid, regionid, gender INTO enrichedpageview FROM pageview LEFT JOIN users ON pageview.userid = users.userid;".toUpperCase());
//        kqlEngine.processStatements("KQL_1", "SELECT userid, pageid, regionid, gender INTO region_pageview FROM enrichedpageview WHERE regionid IS NOT NULL AND regionid = 'Region_5';".toUpperCase());

  }
}
