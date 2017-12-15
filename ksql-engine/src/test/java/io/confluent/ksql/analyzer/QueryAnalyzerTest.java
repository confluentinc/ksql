/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.analyzer;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryAnalyzerTest {

  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore();
  private final KsqlParser ksqlParser = new KsqlParser();
  private final QueryAnalyzer queryAnalyzer =  new QueryAnalyzer(metaStore, new FunctionRegistry());

  @Test
  public void shouldCreateAnalysisForSimpleQuery() {
    final List<Statement> statements = ksqlParser.buildAst("select orderid from orders;", metaStore);
    final Analysis analysis = queryAnalyzer.analyze("sqlExpression", (Query)statements.get(0));
    final Pair<StructuredDataSource, String> fromDataSource = analysis.getFromDataSource(0);
    assertThat(analysis.getSelectExpressions(), equalTo(
        Collections.singletonList(new DereferenceExpression(
            new QualifiedNameReference(QualifiedName.of("ORDERS")), "ORDERID"))));
    assertThat(analysis.getFromDataSources().size(), equalTo(1));
    assertThat(fromDataSource.left, instanceOf(KsqlStream.class));
    assertThat(fromDataSource.right, equalTo("ORDERS"));
  }

  @Test
  public void shouldAnalyseWindowedAggregate() {
    final List<Statement> statements = ksqlParser.buildAst(
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) " +
            "where orderunits > 5 group by itemid;",
        metaStore);
    final Query query = (Query) statements.get(0);
    final Analysis analysis = queryAnalyzer.analyze("sqlExpression",query);
    final AggregateAnalysis aggregateAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);
    final DereferenceExpression itemId = new DereferenceExpression(new QualifiedNameReference(QualifiedName.of("ORDERS")), "ITEMID");
    final DereferenceExpression orderUnits = new DereferenceExpression(new QualifiedNameReference(QualifiedName.of("ORDERS")), "ORDERUNITS");
    final Map<String, Expression> expectedRequiredColumns = new HashMap<>();
    expectedRequiredColumns.put("ORDERS.ITEMID", itemId);
    expectedRequiredColumns.put("ORDERS.ORDERUNITS", orderUnits);
    assertThat(aggregateAnalysis.getNonAggResultColumns(), equalTo(Collections.singletonList(itemId)));
    assertThat(aggregateAnalysis.getFinalSelectExpressions(), equalTo(Arrays.asList(itemId, new QualifiedNameReference(QualifiedName.of("KSQL_AGG_VARIABLE_0")))));
    assertThat(aggregateAnalysis.getAggregateFunctionArguments(), equalTo(Collections.singletonList(orderUnits)));
    assertThat(aggregateAnalysis.getRequiredColumnsMap(), equalTo(expectedRequiredColumns));
  }

  @Test
  public void shouldThrowExceptionIfAggregateAnalysisDoesntHaveGroupBy() {
    final List<Statement> statements = ksqlParser.buildAst(
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) " +
            "where orderunits > 5;",
        metaStore);
    final Query query = (Query) statements.get(0);
    final Analysis analysis = queryAnalyzer.analyze("sqlExpression", query);
    try {
      queryAnalyzer.analyzeAggregate(query, analysis);
      fail("should have thrown KsqlException as aggregate query doesn't have a groupby clause");
    } catch (KsqlException e) {
      // ok
    }

  }

  @Test
  public void shouldThrowExceptionIfNonAggregateSelectsDontMatchGroupBySize() {
    final List<Statement> statements = ksqlParser.buildAst(
        "select itemid, orderid, sum(orderunits) from orders window TUMBLING ( size 30 second) " +
            "where orderunits > 5 group by itemid;",
        metaStore);
    final Query query = (Query) statements.get(0);
    final Analysis analysis = queryAnalyzer.analyze("sqlExpression", query);
    try {
      queryAnalyzer.analyzeAggregate(query, analysis);
      fail("should have thrown KsqlException as aggregate query doesn't have a groupby clause");
    } catch (KsqlException e) {
      // ok
    }
  }

  @Test
  public void shouldProcessHavingExpression() {
    final List<Statement> statements = ksqlParser.buildAst(
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) " +
            "where orderunits > 5 group by itemid having count(itemid) > 10;",
        metaStore);
    final Query query = (Query) statements.get(0);
    final Analysis analysis = queryAnalyzer.analyze("sqlExpression", query);
    final AggregateAnalysis aggregateAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);
    final Expression havingExpression = aggregateAnalysis.getHavingExpression();
    assertThat(havingExpression, equalTo(new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        new QualifiedNameReference(QualifiedName.of("KSQL_AGG_VARIABLE_1")),
        new LongLiteral("10"))));
  }

  @Test
  public void shouldFailWithIncorrectJoinCriteria() {
    final List<Statement> statements = ksqlParser.buildAst(
        "select * from test1 join test2 on test1.col1 = test2.coll;",
        metaStore);
    final Query query = (Query) statements.get(0);
    try {
      queryAnalyzer.analyze("sqlExpression", query);
    } catch (KsqlException ex) {
      assertThat(ex.getMessage().trim(), equalTo("Line: 1, Col: 46 : Invalid join criteria (TEST1.COL1 = TEST2.COLL). Key for TEST2 is not set correctly."));
    }
  }

  @Test
  public void shouldPassJoinWithAnyCriteriaOrder() {
    final List<Statement> statements = ksqlParser.buildAst(
        "select * from test1 left join test2 on test2.col2 = test1.col1;",
        metaStore);
    final Query query = (Query) statements.get(0);
    final Analysis analysis = queryAnalyzer.analyze("sqlExpression", query);
    assertTrue(analysis.getJoin().isLeftJoin());
    assertThat(analysis.getJoin().getLeftKeyFieldName(), equalTo("COL1"));
    assertThat(analysis.getJoin().getRightKeyFieldName(), equalTo("COL2"));
  }

}