/*
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

package io.confluent.ksql.planner;

import static io.confluent.ksql.testutils.AnalysisTestUtil.analyzeQuery;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.util.MetaStoreFixture;
import org.apache.kafka.connect.data.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LogicalPlannerTest {

  private MetaStore metaStore;
  private InternalFunctionRegistry functionRegistry;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    functionRegistry = new InternalFunctionRegistry();
  }

  private PlanNode buildLogicalPlan(final String queryStr) {
    final Analysis analysis = analyzeQuery(queryStr, metaStore);
    final AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    final AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis,
        analysis.getDefaultArgument(), functionRegistry);
    for (final Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, false);
    }
    // Build a logical plan
    return new LogicalPlanner(analysis, aggregateAnalysis, functionRegistry).buildPlan();
  }

  @Test
  public void shouldCreatePlanWithTableAsSource() {
    final PlanNode planNode = buildLogicalPlan("select col0 from TEST2 limit 5;");
    assertThat(planNode.getSources().size(), equalTo(1));
    final StructuredDataSource structuredDataSource = ((StructuredDataSourceNode) planNode
        .getSources()
        .get(0)
        .getSources()
        .get(0))
        .getStructuredDataSource();
    assertThat(structuredDataSource
            .getDataSourceType(),
        equalTo(DataSource.DataSourceType.KTABLE));
    assertThat(structuredDataSource.getName(), equalTo("TEST2"));
  }

  @Test
  public void testSimpleQueryLogicalPlan() {
    final String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan.getSources().get(0), instanceOf(ProjectNode.class));
    assertThat(logicalPlan.getSources().get(0).getSources().get(0), instanceOf(FilterNode.class));
    assertThat(logicalPlan.getSources().get(0).getSources().get(0).getSources().get(0),
        instanceOf(StructuredDataSourceNode.class));

    assertThat(logicalPlan.getSchema().fields().size(), equalTo( 3));
    Assert.assertNotNull(((FilterNode) logicalPlan.getSources().get(0).getSources().get(0)).getPredicate());
  }

  @Test
  public void testSimpleLeftJoinLogicalPlan() {
    final String simpleQuery = "SELECT t1.col1, t2.col1, t1.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan.getSources().get(0), instanceOf(ProjectNode.class));
    assertThat(logicalPlan.getSources().get(0).getSources().get(0), instanceOf(JoinNode.class));
    assertThat(logicalPlan.getSources().get(0).getSources().get(0).getSources()
                          .get(0), instanceOf(StructuredDataSourceNode.class));
    assertThat(logicalPlan.getSources().get(0).getSources().get(0).getSources()
                          .get(1), instanceOf(StructuredDataSourceNode.class));

    assertThat(logicalPlan.getSchema().fields().size(), equalTo(4));

  }

  @Test
  public void testSimpleLeftJoinFilterLogicalPlan() {
    final String
        simpleQuery =
        "SELECT t1.col1, t2.col1, col5, t2.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
        + "t1.col1 = t2.col1 WHERE t1.col1 > 10 AND t2.col4 = 10.8;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan.getSources().get(0), instanceOf(ProjectNode.class));
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    assertThat(projectNode.getKeyField().name(), equalTo("t1.col1".toUpperCase()));
    assertThat(projectNode.getSchema().fields().size(), equalTo(5));

    assertThat(projectNode.getSources().get(0), instanceOf(FilterNode.class));
    final FilterNode filterNode = (FilterNode) projectNode.getSources().get(0);
    assertThat(filterNode.getPredicate().toString(), equalTo("((T1.COL1 > 10) AND (T2.COL4 = 10.8))"));

    assertThat(filterNode.getSources().get(0), instanceOf(JoinNode.class));
    final JoinNode joinNode = (JoinNode) filterNode.getSources().get(0);
    assertThat(joinNode.getSources().get(0), instanceOf(StructuredDataSourceNode.class));
    assertThat(joinNode.getSources().get(1), instanceOf(StructuredDataSourceNode.class));

  }

  @Test
  public void testSimpleAggregateLogicalPlan() {
    final String simpleQuery = "SELECT col0, sum(col3), count(col3) FROM test1 window TUMBLING ( size 2 "
                         + "second) "
                         + "WHERE col0 > 100 GROUP BY col0;";

    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan.getSources().get(0), instanceOf(AggregateNode.class));
    final AggregateNode aggregateNode = (AggregateNode) logicalPlan.getSources().get(0);
    assertThat(aggregateNode.getFunctionList().size(), equalTo(2));
    assertThat(aggregateNode.getFunctionList().get(0).getName().getSuffix(), equalTo("SUM"));
    assertThat(aggregateNode.getWindowExpression().getKsqlWindowExpression().toString(), equalTo(" TUMBLING ( SIZE 2 SECONDS ) "));
    assertThat(aggregateNode.getGroupByExpressions().size(), equalTo(1));
    assertThat(aggregateNode.getGroupByExpressions().get(0).toString(), equalTo("TEST1.COL0"));
    assertThat(aggregateNode.getRequiredColumnList().size(), equalTo(2));
    assertThat(aggregateNode.getSchema().fields().get(1).schema().type(), equalTo(Schema.Type.FLOAT64));
    assertThat(aggregateNode.getSchema().fields().get(2).schema().type(), equalTo(Schema.Type.INT64));
    assertThat(logicalPlan.getSources().get(0).getSchema().fields().size(), equalTo(3));

  }

  @Test
  public void testComplexAggregateLogicalPlan() {
    final String simpleQuery = "SELECT col0, sum(floor(col3)*100)/count(col3) FROM test1 window "
                         + "HOPPING ( size 2 second, advance by 1 second) "
                         + "WHERE col0 > 100 GROUP BY col0;";

    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan.getSources().get(0), instanceOf(AggregateNode.class));
    final AggregateNode aggregateNode = (AggregateNode) logicalPlan.getSources().get(0);
    assertThat(aggregateNode.getFunctionList().size(), equalTo(2));
    assertThat(aggregateNode.getFunctionList().get(0).getName().getSuffix(), equalTo("SUM"));
    assertThat(aggregateNode.getWindowExpression().getKsqlWindowExpression().toString(), equalTo(" HOPPING ( SIZE 2 SECONDS , ADVANCE BY 1 SECONDS ) "));
    assertThat(aggregateNode.getGroupByExpressions().size(), equalTo(1));
    assertThat(aggregateNode.getGroupByExpressions().get(0).toString(), equalTo("TEST1.COL0"));
    assertThat(aggregateNode.getRequiredColumnList().size(), equalTo(2));
    assertThat(aggregateNode.getSchema().fields().get(1).schema().type(), equalTo(Schema.Type.FLOAT64));
    assertThat(logicalPlan.getSources().get(0).getSchema().fields().size(), equalTo(2));

  }

  @Test
  public void shouldCreateTableOutputForAggregateQuery() {
    final String simpleQuery = "SELECT col0, sum(floor(col3)*100)/count(col3) FROM test1 window "
        + "HOPPING ( size 2 second, advance by 1 second) "
        + "WHERE col0 > 100 GROUP BY col0;";

    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KTABLE));
  }

  @Test
  public void shouldCreateStreamOutputForStreamTableJoin() {
    final String
        simpleQuery =
        "SELECT t1.col1, t2.col1, col5, t2.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
            + "t1.col1 = t2.col1 WHERE t1.col1 > 10 AND t2.col4 = 10.8;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KSTREAM));
  }

  @Test
  public void shouldCreateStreamOutputForStreamFilter() {
    final String
        simpleQuery = "SELECT * FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KSTREAM));
  }

  @Test
  public void shouldCreateTableOutputForTableFilter() {
    final String
        simpleQuery = "SELECT * FROM test2 WHERE col4 = 10.8;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KTABLE));
  }

  @Test
  public void shouldCreateStreamOutputForStreamProjection() {
    final String
        simpleQuery = "SELECT col0 FROM test1;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KSTREAM));
  }

  @Test
  public void shouldCreateTableOutputForTableProjection() {
    final String
        simpleQuery = "SELECT col4 FROM test2;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KTABLE));
  }

  @Test
  public void shouldCreateStreamOutputForStreamStreamJoin() {
    final String simpleQuery = "SELECT * FROM ORDERS INNER JOIN TEST1 ON ORDERS.ORDERID=TEST1.COL0;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KSTREAM));
  }

  @Test
  public void shouldCreateTableOutputForTableTableJoin() {
    final String simpleQuery = "SELECT * FROM TEST2 INNER JOIN TEST3 ON TEST2.COL0=TEST3.COL0;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KTABLE));
  }

}
