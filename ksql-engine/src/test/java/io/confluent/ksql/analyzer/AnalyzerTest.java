/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.analyzer;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class AnalyzerTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore();
  }

  private Analysis analyze(String queryStr) {
    List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer("sqlExpression", analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null));
    return analysis;
  }

  @Test
  public void testSimpleQueryAnalysis() throws Exception {
    String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    Analysis analysis = analyze(simpleQuery);
    Assert.assertNotNull("INTO is null", analysis.getInto());
    Assert.assertNotNull("FROM is null", analysis.getFromDataSources());
    Assert.assertNotNull("SELECT is null", analysis.getSelectExpressions());
    Assert.assertNotNull("SELECT aliacs is null", analysis.getSelectExpressionAlias());
    Assert.assertTrue("FROM was not analyzed correctly.",
                      analysis.getFromDataSources().get(0).getLeft().getName()
                          .equalsIgnoreCase("test1"));
    Assert.assertTrue(
        analysis.getSelectExpressions().size() == analysis.getSelectExpressionAlias().size());
    String
        sqlStr =
        SqlFormatter.formatSql(analysis.getWhereExpression()).replace("\n", " ");
    Assert.assertTrue(sqlStr.equalsIgnoreCase("(TEST1.COL0 > 100)"));

    String
        select1 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(0))
            .replace("\n", " ");
    Assert.assertTrue(select1.equalsIgnoreCase("TEST1.COL0"));
    String
        select2 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(1))
            .replace("\n", " ");
    Assert.assertTrue(select2.equalsIgnoreCase("TEST1.COL2"));
    String
        select3 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(2))
            .replace("\n", " ");
    Assert.assertTrue(select3.equalsIgnoreCase("TEST1.COL3"));

    Assert.assertTrue(analysis.getSelectExpressionAlias().get(0).equalsIgnoreCase("COL0"));
    Assert.assertTrue(analysis.getSelectExpressionAlias().get(1).equalsIgnoreCase("COL2"));
    Assert.assertTrue(analysis.getSelectExpressionAlias().get(2).equalsIgnoreCase("COL3"));
  }

  @Test
  public void testSimpleLeftJoinAnalysis() throws Exception {
    String
        simpleQuery =
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
        + "t1.col1 = t2.col1;";
    Analysis analysis = analyze(simpleQuery);
    Assert.assertNotNull("INTO is null", analysis.getInto());
    Assert.assertNotNull("JOIN is null", analysis.getJoin());

    Assert.assertNotNull("SELECT is null", analysis.getSelectExpressions());
    Assert.assertNotNull("SELECT aliacs is null", analysis.getSelectExpressionAlias());
    Assert.assertTrue("JOIN left hand side was not analyzed correctly.",
                      analysis.getJoin().getLeftAlias().equalsIgnoreCase("t1"));
    Assert.assertTrue("JOIN right hand side was not analyzed correctly.",
                      analysis.getJoin().getRightAlias().equalsIgnoreCase("t2"));

    Assert.assertTrue(
        analysis.getSelectExpressions().size() == analysis.getSelectExpressionAlias().size());

    Assert.assertTrue(analysis.getJoin().getLeftKeyFieldName().equalsIgnoreCase("COL1"));
    Assert.assertTrue(analysis.getJoin().getRightKeyFieldName().equalsIgnoreCase("COL1"));

    String
        select1 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(0))
            .replace("\n", " ");
    Assert.assertTrue(select1.equalsIgnoreCase("T1.COL1"));
    String
        select2 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(1))
            .replace("\n", " ");
    Assert.assertTrue(select2.equalsIgnoreCase("T2.COL1"));
    String
        select3 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(2))
            .replace("\n", " ");
    String
        select4 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(3))
            .replace("\n", " ");
    Assert.assertTrue(select3.equalsIgnoreCase("T2.COL4"));
    Assert.assertTrue(select4.equalsIgnoreCase("T1.COL5"));

    Assert.assertTrue(analysis.getSelectExpressionAlias().get(0).equalsIgnoreCase("T1_COL1"));
    Assert.assertTrue(analysis.getSelectExpressionAlias().get(1).equalsIgnoreCase("T2_COL1"));
    Assert.assertTrue(analysis.getSelectExpressionAlias().get(2).equalsIgnoreCase("T2_COL4"));
    Assert.assertTrue(analysis.getSelectExpressionAlias().get(3).equalsIgnoreCase("COL5"));
    Assert.assertTrue(analysis.getSelectExpressionAlias().get(4).equalsIgnoreCase("T2_COL2"));

  }

  @Test
  public void testBooleanExpressionAnalysis() throws Exception {
    String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1;";
    Analysis analysis = analyze(queryStr);

    Assert.assertNotNull("INTO is null", analysis.getInto());
    Assert.assertNotNull("FROM is null", analysis.getFromDataSources());
    Assert.assertNotNull("SELECT is null", analysis.getSelectExpressions());
    Assert.assertNotNull("SELECT aliacs is null", analysis.getSelectExpressionAlias());
    Assert.assertTrue("FROM was not analyzed correctly.",
                      analysis.getFromDataSources().get(0).getLeft().getName()
                          .equalsIgnoreCase("test1"));

    String
        select1 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(0))
            .replace("\n", " ");
    Assert.assertTrue(select1.equalsIgnoreCase("(TEST1.COL0 = 10)"));
    String
        select2 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(1))
            .replace("\n", " ");
    Assert.assertTrue(select2.equalsIgnoreCase("TEST1.COL2"));
    String
        select3 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(2))
            .replace("\n", " ");
    Assert.assertTrue(select3.equalsIgnoreCase("(TEST1.COL3 > TEST1.COL1)"));

  }

  @Test
  public void testFilterAnalysis() throws Exception {
    String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1 WHERE col0 > 20;";
    Analysis analysis = analyze(queryStr);

    Assert.assertNotNull("INTO is null", analysis.getInto());
    Assert.assertNotNull("FROM is null", analysis.getFromDataSources());
    Assert.assertNotNull("SELECT is null", analysis.getSelectExpressions());
    Assert.assertNotNull("SELECT aliacs is null", analysis.getSelectExpressionAlias());
    Assert.assertTrue("FROM was not analyzed correctly.",
            analysis.getFromDataSources().get(0).getLeft().getName()
                    .equalsIgnoreCase("test1"));

    String
            select1 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(0))
                    .replace("\n", " ");
    Assert.assertTrue(select1.equalsIgnoreCase("(TEST1.COL0 = 10)"));
    String
            select2 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(1))
                    .replace("\n", " ");
    Assert.assertTrue(select2.equalsIgnoreCase("TEST1.COL2"));
    String
            select3 =
        SqlFormatter.formatSql(analysis.getSelectExpressions().get(2))
                    .replace("\n", " ");
    Assert.assertTrue(select3.equalsIgnoreCase("(TEST1.COL3 > TEST1.COL1)"));
    Assert.assertTrue("testFilterAnalysis failed.", analysis.getWhereExpression().toString().equalsIgnoreCase("(TEST1.COL0 > 20)"));

  }
}
