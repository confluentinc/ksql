package io.confluent.kql.parser;


import io.confluent.kql.ddl.DDLConfig;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.parser.rewrite.KQLRewriteParser;

import io.confluent.kql.parser.rewrite.SqlFormatterQueryRewrite;
import io.confluent.kql.parser.tree.*;
import io.confluent.kql.serde.json.KQLJsonTopicSerDe;
import io.confluent.kql.util.KQLTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class KQLParserTest {

  private static final KQLParser kqlParser = new KQLParser();

  private static final KQLRewriteParser kqlRewriteParser = new KQLRewriteParser();

  private MetaStore metaStore;

  @Before
  public void init() {

    metaStore = KQLTestUtil.getNewMetaStore();
  }

  @Test
  public void testSimpleQuery() throws Exception {
    String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    Statement statement = kqlParser.buildAST(simpleQuery, metaStore).get(0);


    Assert.assertTrue("testSimpleQuery failes", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testSimpleQuery failes", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    Assert.assertTrue("testSimpleQuery failes", querySpecification.getSelect().getSelectItems().size() == 3);
    Assert.assertTrue("testSimpleQuery failes", querySpecification.getFrom().isPresent());
    Assert.assertTrue("testSimpleQuery failes", querySpecification.getWhere().isPresent());
    Assert.assertTrue("testSimpleQuery failes", querySpecification.getFrom().get() instanceof Relation);
    Assert.assertTrue("testSimpleQuery failes", querySpecification.getWhere().get() instanceof ComparisonExpression);
    ComparisonExpression comparisonExpression = (ComparisonExpression)querySpecification.getWhere().get();
    Assert.assertTrue("testSimpleQuery failes", comparisonExpression.getType().getValue().equalsIgnoreCase(">"));

  }

  @Test
  public void testProjection() throws Exception {
    String queryStr = "SELECT col0, col2, col3 FROM test1;";
    Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
    Assert.assertTrue("testSimpleQuery failes", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testProjection failes", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    Assert.assertTrue("testProjection failes", querySpecification.getSelect().getSelectItems().size() == 3);
    Assert.assertTrue("testProjection fails", querySpecification.getSelect().getSelectItems().get(0) instanceof SingleColumn);
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    Assert.assertTrue("testProjection fails", column0.getAlias().get().equalsIgnoreCase("COL0"));
    Assert.assertTrue("testProjection fails", column0.getExpression().toString().equalsIgnoreCase("TEST1.COL0"));
  }

  @Test
  public void testProjectFilter() throws Exception {
    String queryStr = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
      Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
      Assert.assertTrue("testSimpleQuery failes", statement instanceof Query);
      Query query = (Query) statement;
      Assert.assertTrue("testProjectFilter failes", query.getQueryBody() instanceof QuerySpecification);
      QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();

      Assert.assertTrue("testProjectFilter failes", querySpecification.getWhere().get() instanceof ComparisonExpression);
      ComparisonExpression comparisonExpression = (ComparisonExpression)querySpecification.getWhere().get();
      Assert.assertTrue("testProjectFilter failes", comparisonExpression.toString().equalsIgnoreCase("(TEST1.COL0 > 100)"));
      Assert.assertTrue("testProjectFilter failes", querySpecification.getSelect().getSelectItems().size() == 3);

  }

  @Test
  public void testBinaryExpression() throws Exception {
    String queryStr = "SELECT col0+10, col2, col3-col1 FROM test1;";
    Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
    Assert.assertTrue("testBinaryExpression failes", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testBinaryExpression failes", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    Assert.assertTrue("testBinaryExpression fails", column0.getAlias().get().equalsIgnoreCase("KQL_COL_0"));
    Assert.assertTrue("testBinaryExpression fails", column0.getExpression().toString().equalsIgnoreCase("(TEST1.COL0 + 10)"));
  }

  @Test
  public void testBooleanExpression() throws Exception {
    String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1;";
    Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
    Assert.assertTrue("testBooleanExpression failes", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testProjection failes", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    Assert.assertTrue("testBooleanExpression fails", column0.getAlias().get().equalsIgnoreCase("KQL_COL_0"));
    Assert.assertTrue("testBooleanExpression fails", column0.getExpression().toString().equalsIgnoreCase("(TEST1.COL0 = 10)"));
  }

  @Test
  public void testLiterals() throws Exception {
    String queryStr = "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1;";
    Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
    Assert.assertTrue("testLiterals failes", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testLiterals failes", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    Assert.assertTrue("testLiterals fails", column0.getAlias().get().equalsIgnoreCase("KQL_COL_0"));
    Assert.assertTrue("testLiterals fails", column0.getExpression().toString().equalsIgnoreCase("10"));

    SingleColumn column1 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(1);
    Assert.assertTrue("testLiterals fails", column1.getAlias().get().equalsIgnoreCase("COL2"));
    Assert.assertTrue("testLiterals fails", column1.getExpression().toString().equalsIgnoreCase("TEST1.COL2"));

    SingleColumn column2 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(2);
    Assert.assertTrue("testLiterals fails", column2.getAlias().get().equalsIgnoreCase("KQL_COL_2"));
    Assert.assertTrue("testLiterals fails", column2.getExpression().toString().equalsIgnoreCase("'test'"));

    SingleColumn column3 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(3);
    Assert.assertTrue("testLiterals fails", column3.getAlias().get().equalsIgnoreCase("KQL_COL_3"));
    Assert.assertTrue("testLiterals fails", column3.getExpression().toString().equalsIgnoreCase("2.5"));

    SingleColumn column4 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(4);
    Assert.assertTrue("testLiterals fails", column4.getAlias().get().equalsIgnoreCase("KQL_COL_4"));
    Assert.assertTrue("testLiterals fails", column4.getExpression().toString().equalsIgnoreCase("true"));

    SingleColumn column5 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(5);
    Assert.assertTrue("testLiterals fails", column5.getAlias().get().equalsIgnoreCase("KQL_COL_5"));
    Assert.assertTrue("testLiterals fails", column5.getExpression().toString().equalsIgnoreCase("-5"));
  }

  @Test
  public void testBooleanLogicalExpression() throws Exception {
    String
        queryStr =
        "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1 WHERE col1 = 10 AND col2 LIKE 'val' OR col4 > 2.6 ;";
      Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
      Assert.assertTrue("testSimpleQuery failes", statement instanceof Query);
      Query query = (Query) statement;
      Assert.assertTrue("testProjection failes", query.getQueryBody() instanceof QuerySpecification);
      QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
      SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
      Assert.assertTrue("testProjection fails", column0.getAlias().get().equalsIgnoreCase("KQL_COL_0"));
      Assert.assertTrue("testProjection fails", column0.getExpression().toString().equalsIgnoreCase("10"));

      SingleColumn column1 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(1);
      Assert.assertTrue("testProjection fails", column1.getAlias().get().equalsIgnoreCase("COL2"));
      Assert.assertTrue("testProjection fails", column1.getExpression().toString().equalsIgnoreCase("TEST1.COL2"));

      SingleColumn column2 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(2);
      Assert.assertTrue("testProjection fails", column2.getAlias().get().equalsIgnoreCase("KQL_COL_2"));
      Assert.assertTrue("testProjection fails", column2.getExpression().toString().equalsIgnoreCase("'test'"));

  }

  @Test
  public void testSimpleLeftJoin() throws Exception {
    String
        queryStr =
        "SELECT t1.col1, t2.col1, col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1;";
      Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
      Assert.assertTrue("testSimpleQuery failes", statement instanceof Query);
      Query query = (Query) statement;
      Assert.assertTrue("testSimpleLeftJoin failes", query.getQueryBody() instanceof QuerySpecification);
      QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
      Assert.assertTrue("testSimpleLeftJoin failes", querySpecification.getFrom().get() instanceof Join);
      Join join = (Join) querySpecification.getFrom().get();
      Assert.assertTrue("testSimpleLeftJoin failes", join.getType().toString().equalsIgnoreCase("LEFT"));

      Assert.assertTrue("testSimpleLeftJoin failes", ((AliasedRelation)join.getLeft()).getAlias().equalsIgnoreCase("T1"));
      Assert.assertTrue("testSimpleLeftJoin failes", ((AliasedRelation)join.getRight()).getAlias().equalsIgnoreCase("T2"));

  }

  @Test
  public void testLeftJoinWithFilter() throws Exception {
    String
        queryStr =
        "SELECT t1.col1, t2.col1, col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1 WHERE t2.col2 = 'test';";
      Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
      Assert.assertTrue("testSimpleQuery failes", statement instanceof Query);
      Query query = (Query) statement;
      Assert.assertTrue("testLeftJoinWithFilter failes", query.getQueryBody() instanceof QuerySpecification);
      QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
      Assert.assertTrue("testLeftJoinWithFilter failes", querySpecification.getFrom().get() instanceof Join);
      Join join = (Join) querySpecification.getFrom().get();
      Assert.assertTrue("testLeftJoinWithFilter failes", join.getType().toString().equalsIgnoreCase("LEFT"));

      Assert.assertTrue("testLeftJoinWithFilter failes", ((AliasedRelation)join.getLeft()).getAlias().equalsIgnoreCase("T1"));
      Assert.assertTrue("testLeftJoinWithFilter failes", ((AliasedRelation)join.getRight()).getAlias().equalsIgnoreCase("T2"));

      Assert.assertTrue("testLeftJoinWithFilter failes", querySpecification.getWhere().get().toString().equalsIgnoreCase("(T2.COL2 = 'test')"));
  }

  @Test
  public void testSelectAll() throws Exception {
      String queryStr = "SELECT * FROM test1 t1;";
      Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
      Assert.assertTrue("testSelectAll failes", statement instanceof Query);
      Query query = (Query) statement;
      Assert.assertTrue("testSelectAll failes", query.getQueryBody() instanceof QuerySpecification);
      QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
      Assert.assertTrue("testSelectAll failes", querySpecification.getSelect().getSelectItems().size() == 4);
  }

  @Test
  public void testSelectAllJoin() throws Exception {
    String
        queryStr =
        "SELECT * FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1 WHERE t2.col2 = 'test';";
      Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
      Assert.assertTrue("testSimpleQuery failes", statement instanceof Query);
      Query query = (Query) statement;
      Assert.assertTrue("testLeftJoinWithFilter failes", query.getQueryBody() instanceof QuerySpecification);
      QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
      Assert.assertTrue("testSelectAllJoin failes", querySpecification.getFrom().get() instanceof Join);
      Join join = (Join) querySpecification.getFrom().get();
      Assert.assertTrue("testSelectAllJoin failes", querySpecification.getSelect().getSelectItems().size() == 9);
      Assert.assertTrue("testLeftJoinWithFilter failes", ((AliasedRelation)join.getLeft()).getAlias().equalsIgnoreCase("T1"));
      Assert.assertTrue("testLeftJoinWithFilter failes", ((AliasedRelation)join.getRight()).getAlias().equalsIgnoreCase("T2"));
  }

  @Test
  public void testUDF() throws Exception {
    String queryStr = "SELECT lcase(col1), concat(col2,'hello'), floor(abs(col3)) FROM test1 t1;";
      Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
      Assert.assertTrue("testSelectAll failes", statement instanceof Query);
      Query query = (Query) statement;
      Assert.assertTrue("testSelectAll failes", query.getQueryBody() instanceof QuerySpecification);
      QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();

      SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
      Assert.assertTrue("testProjection fails", column0.getAlias().get().equalsIgnoreCase("KQL_COL_0"));
      Assert.assertTrue("testProjection fails", column0.getExpression().toString().equalsIgnoreCase("LCASE(T1.COL1)"));

      SingleColumn column1 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(1);
      Assert.assertTrue("testProjection fails", column1.getAlias().get().equalsIgnoreCase("KQL_COL_1"));
      Assert.assertTrue("testProjection fails", column1.getExpression().toString().equalsIgnoreCase("CONCAT(T1.COL2, 'hello')"));

      SingleColumn column2 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(2);
      Assert.assertTrue("testProjection fails", column2.getAlias().get().equalsIgnoreCase("KQL_COL_2"));
      Assert.assertTrue("testProjection fails", column2.getExpression().toString().equalsIgnoreCase("FLOOR(ABS(T1.COL3))"));
  }

    @Test
    public void testCreateTopic() throws Exception {
        String
                queryStr =
                "CREATE TOPIC orders_topic WITH (format = 'avro', avroschemafile='/Users/hojjat/avro_order_schema.avro',kafka_topic='orders_topic');";
        Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
        Assert.assertTrue("testCreateTopic failed.", statement instanceof CreateTopic);
        CreateTopic createTopic = (CreateTopic)statement;
        Assert.assertTrue("testCreateTopic failed.", createTopic.getName().toString().equalsIgnoreCase("ORDERS_TOPIC"));
        Assert.assertTrue("testCreateTopic failed.", createTopic.getProperties().size() == 3);
        Assert.assertTrue("testCreateTopic failed.", createTopic.getProperties().get(DDLConfig.FORMAT_PROPERTY).toString().equalsIgnoreCase("'avro'"));

    }

    @Test
    public void testCreateStream() throws Exception {
        String
                queryStr =
                "CREATE STREAM orders (ordertime bigint, orderid varchar, itemid varchar, orderunits double) WITH (topicname = 'orders_topic' , key='ordertime');";
        Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
        Assert.assertTrue("testCreateStream failed.", statement instanceof CreateStream);
        CreateStream createStream = (CreateStream)statement;
        Assert.assertTrue("testCreateStream failed.", createStream.getName().toString().equalsIgnoreCase("ORDERS"));
        Assert.assertTrue("testCreateStream failed.", createStream.getElements().size() == 4);
        Assert.assertTrue("testCreateStream failed.", createStream.getElements().get(0).getName().toString().equalsIgnoreCase("ordertime"));
        Assert.assertTrue("testCreateStream failed.", createStream.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY).toString().equalsIgnoreCase("'orders_topic'"));

    }

    @Test
    public void testCreateTable() throws Exception {
        String
                queryStr =
                "CREATE TABLE users (usertime bigint, userid varchar, regionid varchar, gender varchar) WITH (topicname = 'users_topic', key='userid', statestore='user_statestore');";
        Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
        Assert.assertTrue("testCreateTopic failed.", statement instanceof CreateTable);
        CreateTable createTable = (CreateTable)statement;
        Assert.assertTrue("testCreateTable failed.", createTable.getName().toString().equalsIgnoreCase("USERS"));
        Assert.assertTrue("testCreateTable failed.", createTable.getElements().size() == 4);
        Assert.assertTrue("testCreateTable failed.", createTable.getElements().get(0).getName().toString().equalsIgnoreCase("usertime"));
        Assert.assertTrue("testCreateTable failed.", createTable.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY).toString().equalsIgnoreCase("'users_topic'"));
    }

    @Test
    public void testCreateStreamAsSelect() throws Exception {

        String
                queryStr =
                "CREATE STREAM bigorders_json WITH (format = 'json', kafka_topic='bigorders_topic') AS SELECT * FROM orders WHERE orderunits > 5 ;";
        Statement statement = kqlParser.buildAST(queryStr, metaStore).get(0);
        Assert.assertTrue("testCreateStreamAsSelect failed.", statement instanceof CreateStreamAsSelect);
        CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect)statement;
        Assert.assertTrue("testCreateTable failed.", createStreamAsSelect.getName().toString().equalsIgnoreCase("bigorders_json"));
        Assert.assertTrue("testCreateTable failed.", createStreamAsSelect.getQuery().getQueryBody() instanceof QuerySpecification);
        QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
        Assert.assertTrue("testCreateTable failed.", querySpecification.getSelect().getSelectItems().size() == 4);
        Assert.assertTrue("testCreateTable failed.", querySpecification.getWhere().get().toString().equalsIgnoreCase("(ORDERS.ORDERUNITS > 5)"));
        Assert.assertTrue("testCreateTable failed.", ((AliasedRelation)querySpecification.getFrom().get()).getAlias().equalsIgnoreCase("ORDERS"));
    }


}
