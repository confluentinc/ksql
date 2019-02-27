/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.analyzer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.KsqlParserTestUtil;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class QueryAnalyzerTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final QueryAnalyzer queryAnalyzer =
      new QueryAnalyzer(metaStore, new KsqlConfig(Collections.emptyMap()));

  @Test
  public void shouldCreateAnalysisForSimpleQuery() {
    // Given:
    final Query query = givenQuery("select orderid from orders;");

    // When:
    final Analysis analysis = queryAnalyzer.analyze("sqlExpression", query);

    // Then:
    final Pair<StructuredDataSource, String> fromDataSource = analysis.getFromDataSource(0);
    assertThat(analysis.getSelectExpressions(), equalTo(Collections.singletonList(
        new DereferenceExpression(
            new QualifiedNameReference(QualifiedName.of("ORDERS")), "ORDERID"))));
    assertThat(analysis.getFromDataSources().size(), equalTo(1));
    assertThat(fromDataSource.left, instanceOf(KsqlStream.class));
    assertThat(fromDataSource.right, equalTo("ORDERS"));
  }

  @Test
  public void shouldCreateAnalysisForInserInto() {
    // Given:
    final PreparedStatement<InsertInto> statement = KsqlParserTestUtil.buildSingleAst(
        "insert into test2 select col1 from test1;", metaStore);
    final Query query = statement.getStatement().getQuery();

    // When:
    final Analysis analysis = queryAnalyzer.analyze("sqlExpression", query);

    // Then:
    final Pair<StructuredDataSource, String> fromDataSource = analysis.getFromDataSource(0);
    assertThat(analysis.getSelectExpressions(), equalTo(
        Collections.singletonList(new DereferenceExpression(
            new QualifiedNameReference(QualifiedName.of("TEST1")), "COL1"))));
    assertThat(analysis.getFromDataSources().size(), equalTo(1));
    assertThat(fromDataSource.left, instanceOf(KsqlStream.class));
    assertThat(fromDataSource.right, equalTo("TEST1"));
  }

  @Test
  public void shouldAnalyseWindowedAggregate() {
    // Given:
    final Query query = givenQuery(
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) " +
            "where orderunits > 5 group by itemid;");

    // When:
    final Analysis analysis = queryAnalyzer.analyze("sqlExpression",query);
    final AggregateAnalysis aggregateAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    // Then:
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
  public void shouldThrowIfAggregateAnalysisDoesntHaveGroupBy() {
    // Given:
    final Query query = givenQuery(
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) " +
            "where orderunits > 5;");

    final Analysis analysis = queryAnalyzer.analyze("sqlExpression", query);

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Aggregate query needs GROUP BY clause");

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);
  }

  @Test
  public void shouldThrowOnAdditionalNonAggregateSelects() {
    // Given:
    final Query query = givenQuery(
        "select itemid, orderid, sum(orderunits) from orders group by itemid;");

    final Analysis analysis = queryAnalyzer.analyze("sqlExpression", query);

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Non-aggregate SELECT expression must be part of GROUP BY: [ORDERS.ORDERID]");

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);
  }

  @Test
  public void shouldProcessGroupByExpression() {
    // Given:
    final Query query = givenQuery(
        "select sum(orderunits) from orders group by itemid;");

    final Analysis analysis = queryAnalyzer.analyze("sqlExpression", query);

    // When:
    final AggregateAnalysis aggregateAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    // Then:
    assertThat(aggregateAnalysis.getRequiredColumnsList(),
        hasItem(dereferenceExpression("ORDERS", "ORDERUNITS")));
    assertThat(aggregateAnalysis.getNonAggResultColumns(), is(empty()));
  }

  @Test
  public void shouldProcessHavingExpression() {
    // Given:
    final Query query = givenQuery(
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) " +
            "where orderunits > 5 group by itemid having count(itemid) > 10;");

    final Analysis analysis = queryAnalyzer.analyze("sqlExpression", query);

    // When:
    final AggregateAnalysis aggregateAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    // Then:
    final Expression havingExpression = aggregateAnalysis.getHavingExpression();
    assertThat(havingExpression, equalTo(new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        new QualifiedNameReference(QualifiedName.of("KSQL_AGG_VARIABLE_1")),
        new IntegerLiteral(new NodeLocation(0, 0), 10))));
  }

  @Test
  public void shouldFailWithIncorrectJoinCriteria() {
    // Given:
    final Query query = givenQuery("select * from test1 join test2 on test1.col1 = test2.coll;");

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(containsString(
        "Line: 1, Col: 46 : Invalid join criteria (TEST1.COL1 = TEST2.COLL). "
            + "Could not find a join criteria operand for TEST2."
    ));

    // When:
    queryAnalyzer.analyze("sqlExpression", query);
  }

  @Test
  public void shouldPassJoinWithAnyCriteriaOrder() {
    // Given:
    final Query query = givenQuery(
        "select * from test1 left join test2 on test2.col2 = test1.col1;");

    // When:
    final Analysis analysis = queryAnalyzer.analyze("sqlExpression", query);

    // Then:
    assertTrue(analysis.getJoin().isLeftJoin());
    assertThat(analysis.getJoin().getLeftKeyFieldName(), equalTo("COL1"));
    assertThat(analysis.getJoin().getRightKeyFieldName(), equalTo("COL2"));
  }

  private Query givenQuery(final String sql) {
    return KsqlParserTestUtil.<Query>buildSingleAst(sql, metaStore).getStatement();
  }

  private static Expression dereferenceExpression(final String table, final String field) {
    return new DereferenceExpression(new QualifiedNameReference(QualifiedName.of(table)), field);
  }
}