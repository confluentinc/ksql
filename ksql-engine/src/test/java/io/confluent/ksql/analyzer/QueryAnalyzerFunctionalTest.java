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

import static io.confluent.ksql.util.ExpressionMatchers.qualifiedNameExpressions;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.KsqlParserTestUtil;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * DO NOT ADD NEW TESTS TO THIS FILE
 *
 * <p>Instead add new JSON based tests to QueryTranslationTest
 *
 * <p>This test file is more of a functional test, which is better implemented using QTT.
 */
@SuppressWarnings("OptionalGetWithoutIsPresent")
public class QueryAnalyzerFunctionalTest {

  private static final SourceName ORDERS = SourceName.of("ORDERS");
  private static final SourceName TEST1 = SourceName.of("TEST1");

  private static final ColumnReferenceExp ITEM_ID =
      new ColumnReferenceExp(ColumnRef.of(ORDERS, ColumnName.of("ITEMID")));

  private static final ColumnReferenceExp ORDER_ID =
      new ColumnReferenceExp(ColumnRef.of(ORDERS, ColumnName.of("ORDERID")));

  private static final ColumnReferenceExp ORDER_UNITS =
      new ColumnReferenceExp(ColumnRef.of(ORDERS, ColumnName.of("ORDERUNITS")));

  private static final ColumnReferenceExp TEST_COL1 =
      new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL1")));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final QueryAnalyzer queryAnalyzer =
      new QueryAnalyzer(metaStore, "prefix-~", SerdeOption.none());

  @Test
  public void shouldCreateAnalysisForSimpleQuery() {
    // Given:
    final Query query = givenQuery("select orderid from orders EMIT CHANGES;");

    // When:
    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    // Then:
    final AliasedDataSource fromDataSource = analysis.getFromDataSources().get(0);
    assertThat(
        analysis.getSelectExpressions(),
        contains(SelectExpression.of(ColumnName.of("ORDERID"), ORDER_ID))
    );
    assertThat(analysis.getFromDataSources(), hasSize(1));
    assertThat(fromDataSource.getDataSource(), instanceOf(KsqlStream.class));
    assertThat(fromDataSource.getAlias(), equalTo(SourceName.of("ORDERS")));
  }

  @Test
  public void shouldCreateAnalysisForCsas() {
    // Given:
    final PreparedStatement<CreateStreamAsSelect> statement = KsqlParserTestUtil.buildSingleAst(
        "create stream s as select col1 from test1 EMIT CHANGES;", metaStore);
    final Query query = statement.getStatement().getQuery();
    final Optional<Sink> sink = Optional.of(statement.getStatement().getSink());

    // When:
    final Analysis analysis = queryAnalyzer.analyze(query, sink);

    // Then:
    assertThat(
        analysis.getSelectExpressions(),
        contains(SelectExpression.of(ColumnName.of("COL1"), TEST_COL1))
    );

    assertThat(analysis.getFromDataSources(), hasSize(1));

    final AliasedDataSource fromDataSource = analysis.getFromDataSources().get(0);
    assertThat(fromDataSource.getDataSource(), instanceOf(KsqlStream.class));
    assertThat(fromDataSource.getAlias(), equalTo(SourceName.of("TEST1")));
    assertThat(analysis.getInto().get().getName(), is(SourceName.of("S")));
  }

  @Test
  public void shouldCreateAnalysisForCtas() {
    // Given:
    final PreparedStatement<CreateTableAsSelect> statement = KsqlParserTestUtil.buildSingleAst(
        "create table t as select col1 from test2 EMIT CHANGES;", metaStore);
    final Query query = statement.getStatement().getQuery();
    final Optional<Sink> sink = Optional.of(statement.getStatement().getSink());

    // When:
    final Analysis analysis = queryAnalyzer.analyze(query, sink);

    // Then:
    assertThat(
        analysis.getSelectExpressions(),
        contains(SelectExpression.of(
            ColumnName.of("COL1"),
            new ColumnReferenceExp(ColumnRef.of(SourceName.of("TEST2"), ColumnName.of("COL1")))
        ))
    );

    assertThat(analysis.getFromDataSources(), hasSize(1));

    final AliasedDataSource fromDataSource = analysis.getFromDataSources().get(0);
    assertThat(fromDataSource.getDataSource(), instanceOf(KsqlTable.class));
    assertThat(fromDataSource.getAlias(), equalTo(SourceName.of("TEST2")));
    assertThat(analysis.getInto().get().getName(), is(SourceName.of("T")));
  }

  @Test
  public void shouldCreateAnalysisForInsertInto() {
    // Given:
    final PreparedStatement<InsertInto> statement = KsqlParserTestUtil.buildSingleAst(
        "insert into test0 select col1 from test1 EMIT CHANGES;", metaStore);
    final Query query = statement.getStatement().getQuery();
    final Optional<Sink> sink = Optional.of(statement.getStatement().getSink());

    // When:
    final Analysis analysis = queryAnalyzer.analyze(query, sink);

    // Then:
    assertThat(
        analysis.getSelectExpressions(),
        contains(SelectExpression.of(ColumnName.of("COL1"), TEST_COL1))
    );

    assertThat(analysis.getFromDataSources(), hasSize(1));

    final AliasedDataSource fromDataSource = analysis.getFromDataSources().get(0);
    assertThat(fromDataSource.getDataSource(), instanceOf(KsqlStream.class));
    assertThat(fromDataSource.getAlias(), equalTo(SourceName.of("TEST1")));
    assertThat(analysis.getInto(), is(not(Optional.empty())));
    final Into into = analysis.getInto().get();
    final DataSource<?> test0 = metaStore.getSource(SourceName.of("TEST0"));
    assertThat(into.getName(), is(test0.getName()));
    assertThat(into.getKsqlTopic(), is(test0.getKsqlTopic()));
  }

  @Test
  public void shouldAnalyseWindowedAggregate() {
    // Given:
    final Query query = givenQuery(
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) " +
            "where orderunits > 5 group by itemid EMIT CHANGES;");

    // When:
    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());
    final AggregateAnalysis aggregateAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    // Then:
    assertThat(aggregateAnalysis.getNonAggregateSelectExpressions().get(ITEM_ID), contains(ITEM_ID));
    assertThat(aggregateAnalysis.getFinalSelectExpressions(), equalTo(Arrays.asList(ITEM_ID, new ColumnReferenceExp(
        ColumnRef.withoutSource(ColumnName.of("KSQL_AGG_VARIABLE_0"))))));
    assertThat(aggregateAnalysis.getAggregateFunctionArguments(), equalTo(Collections.singletonList(ORDER_UNITS)));
    assertThat(aggregateAnalysis.getRequiredColumns(), containsInAnyOrder(ITEM_ID, ORDER_UNITS));
  }

  @Test
  public void shouldThrowIfAggregateAnalysisDoesNotHaveGroupBy() {
    // Given:
    final Query query = givenQuery("select itemid, sum(orderunits) from orders EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Use of aggregate functions requires a GROUP BY clause. Aggregate function(s): SUM");

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);
  }

  @Test
  public void shouldThrowOnAdditionalNonAggregateSelects() {
    // Given:
    final Query query = givenQuery(
        "select itemid, orderid, sum(orderunits) from orders group by itemid EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Non-aggregate SELECT expression(s) not part of GROUP BY: [ORDERS.ORDERID]");

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);
  }

  @Test
  public void shouldThrowOnAdditionalNonAggregateHavings() {
    // Given:
    final Query query = givenQuery(
        "select sum(orderunits) from orders group by itemid having orderid = 1 EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    expectedException.expect(KsqlException.class);
    expectedException
        .expectMessage("Non-aggregate HAVING expression not part of GROUP BY: [ORDERS.ORDERID]");

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);
  }

  @Test
  public void shouldProcessGroupByExpression() {
    // Given:
    final Query query = givenQuery(
        "select sum(orderunits) from orders group by itemid EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    // When:
    final AggregateAnalysis aggregateAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    // Then:
    assertThat(aggregateAnalysis.getRequiredColumns(), hasItem(ITEM_ID));
  }

  @Test
  public void shouldProcessGroupByArithmetic() {
    // Given:
    final Query query = givenQuery(
        "select sum(orderunits) from orders group by itemid + 1 EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    // When:
    final AggregateAnalysis aggregateAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    // Then:
    assertThat(aggregateAnalysis.getRequiredColumns(), hasItem(ITEM_ID));
  }

  @Test
  public void shouldProcessGroupByFunction() {
    // Given:
    final Query query = givenQuery(
        "select sum(orderunits) from orders group by ucase(itemid) EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    // When:
    final AggregateAnalysis aggregateAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    // Then:
    assertThat(aggregateAnalysis.getRequiredColumns(), hasItem(ITEM_ID));
  }

  @Test
  public void shouldProcessGroupByConstant() {
    // Given:
    final Query query = givenQuery(
        "select sum(orderunits) from orders group by 1 EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);

    // Then: did not throw.
  }

  @Test
  public void shouldThrowIfGroupByAggFunction() {
    // Given:
    final Query query = givenQuery(
        "select sum(orderunits) from orders group by sum(orderid) EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "GROUP BY does not support aggregate functions: SUM is an aggregate function.");

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);
  }

  @Test
  public void shouldProcessHavingExpression() {
    // Given:
    final Query query = givenQuery(
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) " +
            "where orderunits > 5 group by itemid having count(itemid) > 10 EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    // When:
    final AggregateAnalysis aggregateAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    // Then:
    final Expression havingExpression = aggregateAnalysis.getHavingExpression();
    assertThat(havingExpression, equalTo(new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        new ColumnReferenceExp(ColumnRef.withoutSource(ColumnName.of("KSQL_AGG_VARIABLE_1"))),
        new IntegerLiteral(10))));
  }

  @Test
  public void shouldFailOnSelectStarWithGroupBy() {
    // Given:
    final Query query = givenQuery("select *, count() from orders group by itemid EMIT CHANGES;");
    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Non-aggregate SELECT expression(s) not part of GROUP BY: "
            + "[ORDERS.ADDRESS, ORDERS.ARRAYCOL, ORDERS.ITEMINFO, ORDERS.MAPCOL, ORDERS.ORDERID, "
            + "ORDERS.ORDERTIME, ORDERS.ORDERUNITS, ORDERS.ROWKEY, ORDERS.ROWTIME]"
    );

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);
  }

  @Test
  public void shouldHandleSelectStarWithCorrectGroupBy() {
    // Given:
    final Query query = givenQuery("select *, count() from orders group by "
        + "ROWTIME, ROWKEY, ITEMID, ORDERTIME, ORDERUNITS, MAPCOL, ORDERID, ITEMINFO, ARRAYCOL, ADDRESS"
        + " EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    // When:
    final AggregateAnalysis aggregateAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    // Then:
    assertThat(aggregateAnalysis.getNonAggregateSelectExpressions().keySet(), containsInAnyOrder(
        qualifiedNameExpressions(
            "ORDERS.ROWTIME", "ORDERS.ROWKEY", "ORDERS.ITEMID", "ORDERS.ORDERTIME",
            "ORDERS.ORDERUNITS", "ORDERS.MAPCOL", "ORDERS.ORDERID", "ORDERS.ITEMINFO",
            "ORDERS.ARRAYCOL", "ORDERS.ADDRESS")
    ));
  }

  @Test
  public void shouldThrowIfSelectContainsUdfNotInGroupBy() {
    // Given:
    final Query query = givenQuery("select substring(orderid, 1, 2), count(*) "
        + "from orders group by substring(orderid, 2, 5) EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Non-aggregate SELECT expression(s) not part of GROUP BY: [SUBSTRING(ORDERS.ORDERID, 1, 2)]"
    );

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);
  }

  @Test
  public void shouldThrowIfSelectContainsReversedStringConcatExpression() {
    // Given:
    final Query query = givenQuery("select itemid + address->street, count(*) "
        + "from orders group by address->street + itemid EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Non-aggregate SELECT expression(s) not part of GROUP BY: "
            + "[(ORDERS.ITEMID + ORDERS.ADDRESS->STREET)]"
    );

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);
  }

  @Test
  public void shouldThrowIfSelectContainsFieldsUsedInExpressionInGroupBy() {
    // Given:
    final Query query = givenQuery("select orderId, count(*) "
        + "from orders group by orderid + orderunits EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Non-aggregate SELECT expression(s) not part of GROUP BY: [ORDERS.ORDERID]"
    );

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);
  }

  @Test
  public void shouldThrowIfSelectContainsIncompatibleBinaryArithmetic() {
    // Given:
    final Query query = givenQuery("SELECT orderId - ordertime, COUNT(*) "
        + "FROM ORDERS GROUP BY ordertime - orderId EMIT CHANGES;");

    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Non-aggregate SELECT expression(s) not part of GROUP BY: "
            + "[(ORDERS.ORDERID - ORDERS.ORDERTIME)]"
    );

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);
  }

  @Test
  public void shouldThrowIfGroupByMissingAggregateSelectExpressions() {
    // Given:
    final Query query = givenQuery("select orderid from orders group by orderid EMIT CHANGES;");
    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "GROUP BY requires columns using aggregate functions in SELECT clause."
    );

    // When:
    queryAnalyzer.analyzeAggregate(query, analysis);
  }

  @Test
  public void shouldHandleValueFormat() {
    // Given:
    final PreparedStatement<CreateStreamAsSelect> statement = KsqlParserTestUtil.buildSingleAst(
        "create stream s with(value_format='delimited') as select * from test1;", metaStore);
    final Query query = statement.getStatement().getQuery();
    final Optional<Sink> sink = Optional.of(statement.getStatement().getSink());

    // When:
    final Analysis analysis = queryAnalyzer.analyze(query, sink);

    // Then:
    assertThat(analysis.getInto().get().getKsqlTopic().getValueFormat().getFormat(),
        is(Format.DELIMITED));
  }

  private Query givenQuery(final String sql) {
    return KsqlParserTestUtil.<Query>buildSingleAst(sql, metaStore).getStatement();
  }
}