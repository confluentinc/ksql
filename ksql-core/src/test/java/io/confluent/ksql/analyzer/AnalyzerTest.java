package io.confluent.ksql.analyzer;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.rewrite.SqlFormatterQueryRewrite;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class AnalyzerTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = KsqlTestUtil.getNewMetaStore();
  }

  private Analysis analyze(String queryStr) {
    List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
    System.out.println(SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " "));
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null, null));
    return analysis;
  }

  @Test
  public void testSimpleQueryAnalysis() throws Exception {
    String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    Analysis analysis = analyze(simpleQuery);
    Assert.assertNotNull("INTO is null", analysis.into);
    Assert.assertNotNull("FROM is null", analysis.fromDataSources);
    Assert.assertNotNull("SELECT is null", analysis.selectExpressions);
    Assert.assertNotNull("SELECT aliacs is null", analysis.selectExpressionAlias);
    Assert.assertTrue("FROM was not analyzed correctly.",
                      analysis.fromDataSources.get(0).getLeft().getName()
                          .equalsIgnoreCase("test1"));
    Assert.assertTrue(
        analysis.getSelectExpressions().size() == analysis.getSelectExpressionAlias().size());
    String
        sqlStr =
        SqlFormatterQueryRewrite.formatSql(analysis.getWhereExpression()).replace("\n", " ");
    Assert.assertTrue(sqlStr.equalsIgnoreCase("(TEST1.COL0 > 100)"));

    String
        select1 =
        SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(0))
            .replace("\n", " ");
    Assert.assertTrue(select1.equalsIgnoreCase("TEST1.COL0"));
    String
        select2 =
        SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(1))
            .replace("\n", " ");
    Assert.assertTrue(select2.equalsIgnoreCase("TEST1.COL2"));
    String
        select3 =
        SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(2))
            .replace("\n", " ");
    Assert.assertTrue(select3.equalsIgnoreCase("TEST1.COL3"));

    Assert.assertTrue(analysis.selectExpressionAlias.get(0).equalsIgnoreCase("COL0"));
    Assert.assertTrue(analysis.selectExpressionAlias.get(1).equalsIgnoreCase("COL2"));
    Assert.assertTrue(analysis.selectExpressionAlias.get(2).equalsIgnoreCase("COL3"));
  }

  @Test
  public void testSimpleLeftJoinAnalysis() throws Exception {
    String
        simpleQuery =
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
        + "t1.col1 = t2.col1;";
    Analysis analysis = analyze(simpleQuery);
    Assert.assertNotNull("INTO is null", analysis.into);
    Assert.assertNotNull("JOIN is null", analysis.join);

    Assert.assertNotNull("SELECT is null", analysis.selectExpressions);
    Assert.assertNotNull("SELECT aliacs is null", analysis.selectExpressionAlias);
    Assert.assertTrue("JOIN left hand side was not analyzed correctly.",
                      analysis.join.getLeftAlias().equalsIgnoreCase("t1"));
    Assert.assertTrue("JOIN right hand side was not analyzed correctly.",
                      analysis.join.getRightAlias().equalsIgnoreCase("t2"));

    Assert.assertTrue(
        analysis.getSelectExpressions().size() == analysis.getSelectExpressionAlias().size());

    Assert.assertTrue(analysis.join.getLeftKeyFieldName().equalsIgnoreCase("COL1"));
    Assert.assertTrue(analysis.join.getRightKeyFieldName().equalsIgnoreCase("COL1"));

    String
        select1 =
        SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(0))
            .replace("\n", " ");
    Assert.assertTrue(select1.equalsIgnoreCase("T1.COL1"));
    String
        select2 =
        SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(1))
            .replace("\n", " ");
    Assert.assertTrue(select2.equalsIgnoreCase("T2.COL1"));
    String
        select3 =
        SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(2))
            .replace("\n", " ");
    String
        select4 =
        SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(3))
            .replace("\n", " ");
    Assert.assertTrue(select3.equalsIgnoreCase("T2.COL4"));
    Assert.assertTrue(select4.equalsIgnoreCase("T1.COL5"));

    Assert.assertTrue(analysis.selectExpressionAlias.get(0).equalsIgnoreCase("T1_COL1"));
    Assert.assertTrue(analysis.selectExpressionAlias.get(1).equalsIgnoreCase("T2_COL1"));
    Assert.assertTrue(analysis.selectExpressionAlias.get(2).equalsIgnoreCase("T2_COL4"));
    Assert.assertTrue(analysis.selectExpressionAlias.get(3).equalsIgnoreCase("COL5"));
    Assert.assertTrue(analysis.selectExpressionAlias.get(4).equalsIgnoreCase("T2_COL2"));

  }

  @Test
  public void testBooleanExpressionAnalysis() throws Exception {
    String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1;";
    Analysis analysis = analyze(queryStr);

    Assert.assertNotNull("INTO is null", analysis.into);
    Assert.assertNotNull("FROM is null", analysis.fromDataSources);
    Assert.assertNotNull("SELECT is null", analysis.selectExpressions);
    Assert.assertNotNull("SELECT aliacs is null", analysis.selectExpressionAlias);
    Assert.assertTrue("FROM was not analyzed correctly.",
                      analysis.fromDataSources.get(0).getLeft().getName()
                          .equalsIgnoreCase("test1"));

    String
        select1 =
        SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(0))
            .replace("\n", " ");
    Assert.assertTrue(select1.equalsIgnoreCase("(TEST1.COL0 = 10)"));
    String
        select2 =
        SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(1))
            .replace("\n", " ");
    Assert.assertTrue(select2.equalsIgnoreCase("TEST1.COL2"));
    String
        select3 =
        SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(2))
            .replace("\n", " ");
    Assert.assertTrue(select3.equalsIgnoreCase("(TEST1.COL3 > TEST1.COL1)"));

  }

  @Test
  public void testFilterAnalysis() throws Exception {
    String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1 WHERE col0 > 20;";
    Analysis analysis = analyze(queryStr);

    Assert.assertNotNull("INTO is null", analysis.into);
    Assert.assertNotNull("FROM is null", analysis.fromDataSources);
    Assert.assertNotNull("SELECT is null", analysis.selectExpressions);
    Assert.assertNotNull("SELECT aliacs is null", analysis.selectExpressionAlias);
    Assert.assertTrue("FROM was not analyzed correctly.",
            analysis.fromDataSources.get(0).getLeft().getName()
                    .equalsIgnoreCase("test1"));

    String
            select1 =
            SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(0))
                    .replace("\n", " ");
    Assert.assertTrue(select1.equalsIgnoreCase("(TEST1.COL0 = 10)"));
    String
            select2 =
            SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(1))
                    .replace("\n", " ");
    Assert.assertTrue(select2.equalsIgnoreCase("TEST1.COL2"));
    String
            select3 =
            SqlFormatterQueryRewrite.formatSql(analysis.getSelectExpressions().get(2))
                    .replace("\n", " ");
    Assert.assertTrue(select3.equalsIgnoreCase("(TEST1.COL3 > TEST1.COL1)"));
    Assert.assertTrue("testFilterAnalysis failed.", analysis.getWhereExpression().toString().equalsIgnoreCase("(TEST1.COL0 > 20)"));

  }
}
