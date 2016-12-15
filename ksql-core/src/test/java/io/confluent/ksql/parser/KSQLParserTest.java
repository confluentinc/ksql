package io.confluent.ksql.parser;


import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KafkaTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.rewrite.KSQLRewriteParser;
import io.confluent.ksql.parser.rewrite.SqlFormatterQueryRewrite;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.util.KSQLTestUtil;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class KSQLParserTest {

  private static final KSQLParser ksqlParser = new KSQLParser();

  private static final KSQLRewriteParser ksqlRewriteParser = new KSQLRewriteParser();

  private MetaStore metaStore;

  @Before
  public void init() {

    metaStore = KSQLTestUtil.getNewMetaStore();
  }

//  @Test
//  public void testSimpleQuery() throws Exception {
//    String simpleQuery = "SELECT col0, col2, col3 INTO testOutput FROM test1 WHERE col0 > 100;";
//    List<Statement> statements = ksqlParser.buildAST(simpleQuery, metaStore);
//
//    String sqlStr = SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " ");
//    Assert.assertTrue("testSimpleQuery failes", sqlStr.equalsIgnoreCase(
//        "SELECT   TEST1.COL0  AS COL0 , TEST1.COL2  AS COL2 , TEST1.COL3  AS COL3 INTO   TESTOUTPUT FROM   TEST1 TEST1 WHERE (TEST1.COL0 > 100) "));
//  }
//
//
//  public void testProjection() throws Exception {
//    String queryStr = "SELECT col0, col2, col3 INTO testOutput FROM test1;";
//    List<Statement> statements = ksqlParser.buildAST(queryStr, metaStore);
//    String sqlStr = SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " ");
//    Assert.assertTrue("testProjection failes", sqlStr.equalsIgnoreCase(
//        "SELECT   TEST1.COL0  AS COL0 , TEST1.COL2  AS COL2 , TEST1.COL3  AS COL3 INTO   TESTOUTPUT FROM   TEST1 TEST1 "));
//  }
//
//  @Test
//  public void testProjectFilter() throws Exception {
//    String queryStr = "SELECT col0, col2, col3 INTO testOutput FROM test1 WHERE col0 > 100;";
//    List<Statement> statements = ksqlParser.buildAST(queryStr, metaStore);
//    String sqlStr = SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " ");
//    Assert.assertTrue("testProjectFilter failes", sqlStr.equalsIgnoreCase(
//        "SELECT   TEST1.COL0  AS COL0 , TEST1.COL2  AS COL2 , TEST1.COL3  AS COL3 INTO   TESTOUTPUT FROM   TEST1 TEST1 WHERE (TEST1.COL0 > 100) "));
//  }
//
//  @Test
//  public void testBinaryExpression() throws Exception {
//    String queryStr = "SELECT col0+10, col2, col3-col1 INTO testOutput FROM test1;";
//    List<Statement> statements = ksqlParser.buildAST(queryStr, metaStore);
//    String sqlStr = SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " ");
//    Assert.assertTrue("testBinaryExpression failes", sqlStr.equalsIgnoreCase(
//        "SELECT   (TEST1.COL0 + 10)  AS KSQL_COL_0 , TEST1.COL2  AS COL2 , (TEST1.COL3 - TEST1.COL1)  AS KSQL_COL_2 INTO   TESTOUTPUT FROM   TEST1 TEST1 "));
//  }
//
//  @Test
//  public void testBooleanExpression() throws Exception {
//    String queryStr = "SELECT col0 = 10, col2, col3 > col1 INTO testOutput FROM test1;";
//    List<Statement> statements = ksqlParser.buildAST(queryStr, metaStore);
//    String sqlStr = SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " ");
//    Assert.assertTrue("testBooleanExpression failes", sqlStr.equalsIgnoreCase(
//        "SELECT   (TEST1.COL0 = 10)  AS KSQL_COL_0 , TEST1.COL2  AS COL2 , (TEST1.COL3 > TEST1.COL1)  AS KSQL_COL_2 INTO   TESTOUTPUT FROM   TEST1 TEST1 "));
//  }
//
//  @Test
//  public void testLiterals() throws Exception {
//    String queryStr = "SELECT 10, col2, 'test', 2.5, true, -5 INTO testOutput FROM test1;";
//    List<Statement> statements = ksqlParser.buildAST(queryStr, metaStore);
//    String sqlStr = SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " ");
//    Assert.assertTrue("testLiterals failes", sqlStr.equalsIgnoreCase(
//        "SELECT   10  AS KSQL_COL_0 , TEST1.COL2  AS COL2 , 'test'  AS KSQL_COL_2 , 2.5  AS KSQL_COL_3 , true  AS KSQL_COL_4 , -5  AS KSQL_COL_5 INTO   TESTOUTPUT FROM   TEST1 TEST1 "));
//  }
//
//  @Test
//  public void testBooleanLogicalExpression() throws Exception {
//    String
//        queryStr =
//        "SELECT 10, col2, 'test', 2.5, true, -5 INTO testOutput FROM test1 WHERE col1 = 10 AND col2 LIKE 'val' OR col4 > 2.6 ;";
//    List<Statement> statements = ksqlParser.buildAST(queryStr, metaStore);
//    String sqlStr = SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " ");
//    Assert.assertTrue("testBooleanLogicalExpression failes", sqlStr.equalsIgnoreCase(
//        "SELECT   10  AS KSQL_COL_0 , TEST1.COL2  AS COL2 , 'test'  AS KSQL_COL_2 , 2.5  AS KSQL_COL_3 , true  AS KSQL_COL_4 , -5  AS KSQL_COL_5 INTO   TESTOUTPUT FROM   TEST1 TEST1 WHERE (((TEST1.COL1 = 10) AND (TEST1.COL2 LIKE 'val')) OR (TEST1.COL4 > 2.6)) "));
//  }
//
//  @Test
//  public void testSimpleLeftJoin() throws Exception {
//    String
//        queryStr =
//        "SELECT t1.col1, t2.col1, col4, t2.col2 INTO testOutput FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1;";
//    List<Statement> statements = ksqlParser.buildAST(queryStr, metaStore);
//    String sqlStr = SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " ");
//    Assert.assertTrue("testSimpleLeftJoin failes", sqlStr.equalsIgnoreCase(
//        "SELECT   T1.COL1  AS T1_COL1 , T2.COL1  AS T2_COL1 , T2.COL4  AS COL4 , T2.COL2  AS T2_COL2 INTO   TESTOUTPUT FROM   TEST1 T1 LEFT JOIN TEST2 T2 ON ((T1.COL1 = T2.COL1)) "));
//  }
//
//  @Test
//  public void testLeftJoinWithFilter() throws Exception {
//    String
//        queryStr =
//        "SELECT t1.col1, t2.col1, col4, t2.col2 INTO testOutput FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1 WHERE t2.col2 = 'test';";
//    List<Statement> statements = ksqlParser.buildAST(queryStr, metaStore);
//    String sqlStr = SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " ");
//    Assert.assertTrue("testLeftJoinWithFilter failes", sqlStr.equalsIgnoreCase(
//        "SELECT   T1.COL1  AS T1_COL1 , T2.COL1  AS T2_COL1 , T2.COL4  AS COL4 , T2.COL2  AS T2_COL2 INTO   TESTOUTPUT FROM   TEST1 T1 LEFT JOIN TEST2 T2 ON ((T1.COL1 = T2.COL1)) WHERE (T2.COL2 = 'test') "));
//  }
//
//  @Test
//  public void testSelectAll() throws Exception {
//    String queryStr = "SELECT * INTO testOutput FROM test1 t1;";
//    List<Statement> statements = ksqlParser.buildAST(queryStr, metaStore);
//    String sqlStr = SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " ");
//    Assert.assertTrue("testSelectAll failes", sqlStr.equalsIgnoreCase(
//        "SELECT   TEST1.COL0  AS col0 , TEST1.COL1  AS col1 , TEST1.COL2  AS col2 , TEST1.COL3  AS col3 INTO   TESTOUTPUT FROM   TEST1 T1 "));
//  }
//
//  @Test
//  public void testSelectAllJoin() throws Exception {
//    String
//        queryStr =
//        "SELECT * INTO testOutput FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1 WHERE t2.col2 = 'test';";
//    List<Statement> statements = ksqlParser.buildAST(queryStr, metaStore);
//    String sqlStr = SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " ");
//    Assert.assertTrue("testSelectAllJoin failes", sqlStr.equalsIgnoreCase(
//        "SELECT   T1.COL0  AS T1_COL0 , T1.COL1  AS T1_COL1 , T1.COL2  AS T1_COL2 , T1.COL3  AS T1_COL3 , T2.COL0  AS T2_COL0 , T2.COL1  AS T2_COL1 , T2.COL2  AS T2_COL2 , T2.COL3  AS T2_COL3 , T2.COL4  AS T2_COL4 INTO   TESTOUTPUT FROM   TEST1 T1 LEFT JOIN TEST2 T2 ON ((T1.COL1 = T2.COL1)) WHERE (T2.COL2 = 'test') "));
//  }
}
