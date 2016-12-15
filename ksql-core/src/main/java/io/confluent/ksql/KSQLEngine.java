package io.confluent.ksql;


import io.confluent.ksql.ddl.DDLEngine;
import io.confluent.ksql.metastore.*;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class KSQLEngine {

  KSQLConfig ksqlConfig;

  QueryEngine queryEngine;
  DDLEngine ddlEngine = new DDLEngine(this);


  MetaStore metaStore = null;

  public List<Triplet<String, KafkaStreams, OutputNode>> runMultipleQueries(
      String queriesString) throws Exception {

    // Parse and AST creation
    KSQLParser ksqlParser = new KSQLParser();
    List<SqlBaseParser.SingleStatementContext>
        parsedStatements =
        ksqlParser.getStatements(queriesString);
    int queryIndex = 0;
    List<Pair<String, Query>> queryList = new ArrayList<>();
    MetaStore tempMetaStore = new MetaStoreImpl();
    for (String dataSourceName : metaStore.getAllStructuredDataSource().keySet()) {
      tempMetaStore.putSource(metaStore.getSource(dataSourceName));
    }
    for (SqlBaseParser.SingleStatementContext singleStatementContext : parsedStatements) {
      Pair<Statement, DataSourceExtractor>
          statementInfo =
          ksqlParser.prepareStatement(singleStatementContext, tempMetaStore);
      Statement statement = statementInfo.getLeft();
      if (statement instanceof Query) {
        Query query = (Query) statement;
        queryList.add(new Pair("KSQL_QUERY_" + queryIndex, query));
        queryIndex++;
      } else if (statement instanceof CreateTopic) {
        ddlEngine.createTopic((CreateTopic) statement);
      } else if (statement instanceof DropTable) {
        ddlEngine.dropTopic((DropTable) statement);
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

  public void runCLIQuery(String queriesString, long terminateIn) throws Exception {
    // Parse and AST creation
    KSQLParser ksqlParser = new KSQLParser();
    List<SqlBaseParser.SingleStatementContext>
        parsedStatements =
        ksqlParser.getStatements(queriesString);
    int queryIndex = 0;
    List<Pair<String, Query>> queryList = new ArrayList<>();
    MetaStore tempMetaStore = new MetaStoreImpl();
    for (String dataSourceName : metaStore.getAllStructuredDataSource().keySet()) {
      tempMetaStore.putSource(metaStore.getSource(dataSourceName));
    }
    for (SqlBaseParser.SingleStatementContext singleStatementContext : parsedStatements) {
      Pair<Statement, DataSourceExtractor>
          statementInfo =
          ksqlParser.prepareStatement(singleStatementContext, tempMetaStore);
      Statement statement = statementInfo.getLeft();
      if (statement instanceof Query) {
        Query query = (Query) statement;
        queryList.add(new Pair("KSQL_QUERY_" + queryIndex, query));
        queryIndex++;
      } else if (statement instanceof CreateTopic) {
        ddlEngine.createTopic((CreateTopic) statement);
      } else if (statement instanceof DropTable) {
        ddlEngine.dropTopic((DropTable) statement);
      }
    }

    // Logical plan creation from the ASTs
    List<Pair<String, PlanNode>> logicalPlans = queryEngine.buildLogicalPlans(metaStore, queryList);

    // Physical plan creation from logical plans.
    queryEngine.buildRunSingleConsolePhysicalPlans(metaStore, logicalPlans.get(0), terminateIn);


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
    KSQLParser ksqlParser = new KSQLParser();
    List<Statement> builtASTStatements = ksqlParser.buildAST(sqlString, metaStore);
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


  public MetaStore getMetaStore() {
    return metaStore;
  }

  public QueryEngine getQueryEngine() {
    return queryEngine;
  }

  public DDLEngine getDdlEngine() {
    return ddlEngine;
  }

  public KSQLConfig getKsqlConfig() {
    return ksqlConfig;
  }

  public KSQLEngine(String schemaFilePath) throws IOException {
    this.metaStore = new MetastoreUtil().loadMetastoreFromJSONFile(schemaFilePath);
  }

  public KSQLEngine(Map<String, String> ksqlConfProperties) throws IOException {
    String schemaPath = ksqlConfProperties.get(KSQLConfig.SCHEMA_FILE_PATH_CONFIG);
    Properties ksqlProperties = new Properties();
    ksqlProperties
        .put(StreamsConfig.APPLICATION_ID_CONFIG, "KSQL-Default-" + System.currentTimeMillis());
    ksqlProperties
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KSQLConfig.DEFAULT_BOOTSTRAP_SERVERS_CONFIG);
    ksqlProperties
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KSQLConfig.DEFAULT_AUTO_OFFSET_RESET_CONFIG);
    if (!ksqlConfProperties.get(KSQLConfig.PROP_FILE_PATH_CONFIG)
        .equalsIgnoreCase(KSQLConfig.DEFAULT_PROP_FILE_PATH_CONFIG)) {
      ksqlProperties.load(new FileReader(ksqlConfProperties.get(KSQLConfig.PROP_FILE_PATH_CONFIG)));
    }

    this.ksqlConfig = new KSQLConfig(ksqlProperties);
    this.metaStore = new MetastoreUtil().loadMetastoreFromJSONFile(schemaPath);
    this.queryEngine = new QueryEngine(this.ksqlConfig);
  }

  public static void main(String[] args) throws Exception {
    Map<String, String> ksqlConfProperties = new HashMap<>();
//    ksqlConfProperties.put(KSQLConfig.SCHEMA_FILE_PATH_CONFIG, "/Users/hojjat/userschema.json");
    ksqlConfProperties.put(KSQLConfig.SCHEMA_FILE_PATH_CONFIG, "/tmp/ksql/schema.json");
    ksqlConfProperties.put(KSQLConfig.PROP_FILE_PATH_CONFIG, "/Users/hojjat/ksql_config.conf");
    KSQLEngine ksqlEngine = new KSQLEngine(ksqlConfProperties);

//    ksqlEngine.runCLIQuery("SELECT ordertime AS timeValue, orderid , orderunits%10, lower(itemid)"
//                           + "  "
//                           + "FROM orders;"
//                           + "", -1);

//    ksqlEngine.processStatements("KSQL_1","SELECT ordertime AS timeValue, orderid , "
//                                          + "orderunits%10, lcase(itemid) FROM orders");

//    ksqlEngine.processStatements("KSQL_1","SELECT lcase(itemid) FROM orders");

//    ksqlEngine.processStatements("KSQL_1","SELECT len(orderid), CAST(substring(itemid,5)  AS "
//                                          + "INTEGER) FROM orders "
//                                          + "WHERE "
//                                          + "itemid "
//                                          + "LIKE "
//                                          + "'%5'");
//    ksqlEngine.processStatements("KSQL_1","SELECT ordertime AS timeValue, orderid , orderunits%10 into stream5 FROM orders WHERE NOT (orderunits > 5) ;");
//    ksqlEngine.processStatements("KSQL_1","SELECT ordertime AS timeValue, orderid , orderunits%10"
//                                          + " into stream1 FROM orders ;");
//    ksqlEngine.processStatements("KSQL_1","SELECT USERID, REGIONID "
//                                          + "  FROM users where userid = 'User_65';");
//    ksqlEngine.processStatements("KSQL_1","SELECT USERID, PAGEID "
//                                          + "  FROM pageview where userid = 'User_65';");

    ksqlEngine.processStatements("KSQL_1","SELECT * "
                                          + "  FROM users ;");
//    ksqlEngine.processStatements("KSQL_1","SELECT * "
//                                          + "  FROM orders ;");

//    ksqlEngine.processStatements("KSQL_1", "select pageview.USERID, users.USERID, PAGEID, REGIONID, VIEWTIME FROM pageview LEFT JOIN users ON pageview.USERID = users.USERID;");

//        ksqlEngine.processStatements("CREATE TOPIC orders ( orderkey bigint, orderstatus varchar, totalprice double, orderdate date)".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1","SELECT ordertime AS timeValue, orderid , orderunits%10 into stream5 FROM orders WHERE NOT (orderunits > 5) ;");
//        ksqlEngine.processStatements("KSQL_1","SELECT ordertime AS timeValue, orderid , orderunits%10 into stream5 FROM orders WHERE orderid IS NOT NULL ;");
//        ksqlEngine.processStatements("KSQL_1", "select o.ordertime+1, o.itemId, orderunits into stream1 from orders o where o.orderunits > 5;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select * into stream1 from orders JOIN shipment ON orderid = shipmentorderid where orderunits > 5;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select u.userid, p.pageid , p.viewtime, regionid into stream3 from  pageview p LEFT JOIN users u ON u.userid = p.userid;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select u.userid, p.userid, p.pageid , p.viewtime, regionid into stream1 from  pageview p JOIN users u ON u.userid = p.userid;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select pageview.USERID, users.USERID, PAGEID, REGIONID, VIEWTIME into stream2 FROM pageview JOIN users ON pageview.USERID = users.USERID;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select USERID, REGIONID into stream8 from users where REGIONID = 'Region_5';".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select USERID, REGIONID, userid = '*****', 12 into stream11 from users;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select * into ktable1 from users;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select * from orders;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select pageview.USERID, users.USERID, PAGEID, REGIONID, VIEWTIME into stream6 FROM pageview LEFT JOIN users ON pageview.USERID = users.USERID;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select ORDERTIME, ITEMID, ORDERUNITS into stream6 from orders where ORDERUNITS > 8 AND ITEMID = 'Item_3';".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select users.userid, pageid, regionid, gender into stream3 from pageview left join users on pageview.userid = users.userid;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "SELECT PAGEVIEW.USERID, PAGEID, REGIONID, GENDER INTO PAGEVIEWJOIN1 FROM PAGEVIEW LEFT JOIN USERS ON PAGEVIEW.USERID = USERS.USERID;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select USERTIME, USERID, REGIONID into stream5 from users;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select ordertime, itemId, orderunits, '**===*' AS t into stream3 from orders;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "SELECT users.userid AS userid, pageid, regionid, gender INTO enrichedpageview FROM pageview LEFT JOIN users ON pageview.userid = users.userid;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "SELECT userid, pageid, regionid, gender INTO region_pageview FROM enrichedpageview WHERE regionid IS NOT NULL AND regionid = 'Region_5';".toUpperCase());

  }
}
