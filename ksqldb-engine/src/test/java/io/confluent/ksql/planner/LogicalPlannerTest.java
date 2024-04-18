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

package io.confluent.ksql.planner;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.FinalProjectNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PreJoinRepartitionNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.QueryLimitNode;
import io.confluent.ksql.planner.plan.UserRepartitionNode;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class LogicalPlannerTest {

  private MetaStore metaStore;
  private KsqlConfig ksqlConfig;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(TestFunctionRegistry.INSTANCE.get());
    ksqlConfig = new KsqlConfig(ImmutableMap.of(KsqlConfig.KSQL_SUPPRESS_ENABLED, true));
  }

  @Test
  public void shouldCreatePlanWithTableAsSource() {
    final PlanNode planNode = buildLogicalPlan("select col0 from TEST2 EMIT CHANGES limit 5;");
    assertThat(planNode.getSources().size(), equalTo(1));
    final DataSource dataSource = ((DataSourceNode) planNode
        .getSources()
        .get(0)
        .getSources()
        .get(0)
        .getSources()
        .get(0))
        .getDataSource();
    assertThat(dataSource
            .getDataSourceType(),
        equalTo(DataSourceType.KTABLE));
    assertThat(dataSource.getName(), equalTo(SourceName.of("TEST2")));
  }

  @Test
  public void testSimpleQueryLogicalPlan() {
    final String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan.getSources().get(0), instanceOf(ProjectNode.class));
    assertThat(logicalPlan.getSources().get(0).getSources().get(0), instanceOf(FilterNode.class));
    assertThat(logicalPlan.getSources().get(0).getSources().get(0).getSources().get(0),
        instanceOf(DataSourceNode.class));

    assertThat(logicalPlan.getSchema().value().size(), equalTo( 3));
    Assert.assertNotNull(((FilterNode) logicalPlan.getSources().get(0).getSources().get(0)).getPredicate());
  }

  @Test
  public void testSimpleLeftJoinLogicalPlan() {
    final String simpleQuery = "SELECT t1.col1, t2.col1, t1.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col0 = t2.col0 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan.getSources().get(0), instanceOf(ProjectNode.class));
    assertThat(logicalPlan.getSources().get(0).getSources().get(0), instanceOf(JoinNode.class));
    final PlanNode leftSource =
        logicalPlan.getSources().get(0).getSources().get(0).getSources().get(0);
    assertThat(leftSource, instanceOf(ProjectNode.class));
    assertThat(leftSource.getSources().get(0), instanceOf(PreJoinRepartitionNode.class));
    final PlanNode rightSource =
        logicalPlan.getSources().get(0).getSources().get(0).getSources().get(1);
    assertThat(rightSource, instanceOf(ProjectNode.class));
    assertThat(rightSource.getSources().get(0), instanceOf(PreJoinRepartitionNode.class));

    assertThat(logicalPlan.getSchema().value().size(), equalTo(4));
  }

  @Test
  public void testSimpleLeftJoinFilterLogicalPlan() {
    final String
        simpleQuery =
        "SELECT t1.col1, t2.col1, col5, t2.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
        + "t1.col0 = t2.col0 WHERE t1.col3 > 10.8 AND t2.col2 = 'foo' EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan.getSources().get(0), instanceOf(ProjectNode.class));
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    assertThat(projectNode.getSchema().value().size(), equalTo(5));

    assertThat(projectNode.getSources().get(0), instanceOf(FilterNode.class));
    final FilterNode filterNode = (FilterNode) projectNode.getSources().get(0);
    assertThat(filterNode.getPredicate().toString(), equalTo("((T1_COL3 > 10.8) AND (T2_COL2 = 'foo'))"));

    assertThat(filterNode.getSources().get(0), instanceOf(JoinNode.class));
    final JoinNode joinNode = (JoinNode) filterNode.getSources().get(0);
    final PlanNode leftSource = joinNode.getSources().get(0);
    assertThat(leftSource, instanceOf(ProjectNode.class));
    assertThat(leftSource.getSources().get(0), instanceOf(PreJoinRepartitionNode.class));
    final PlanNode rightSource = joinNode.getSources().get(0);
    assertThat(rightSource, instanceOf(ProjectNode.class));
    assertThat(rightSource.getSources().get(0), instanceOf(PreJoinRepartitionNode.class));
  }

  @Test
  public void testSimpleRightJoinLogicalPlan() {
    final String simpleQuery = "SELECT t1.col1, t2.col1, t1.col4, t2.col2 FROM test1 t1 RIGHT JOIN test2 t2 ON t1.col0 = t2.col0 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan.getSources().get(0), instanceOf(ProjectNode.class));
    assertThat(logicalPlan.getSources().get(0).getSources().get(0), instanceOf(JoinNode.class));
    final PlanNode leftSource =
        logicalPlan.getSources().get(0).getSources().get(0).getSources().get(0);
    assertThat(leftSource, instanceOf(ProjectNode.class));
    assertThat(leftSource.getSources().get(0), instanceOf(PreJoinRepartitionNode.class));
    final PlanNode rightSource =
        logicalPlan.getSources().get(0).getSources().get(0).getSources().get(1);
    assertThat(rightSource, instanceOf(ProjectNode.class));
    assertThat(rightSource.getSources().get(0), instanceOf(PreJoinRepartitionNode.class));

    assertThat(logicalPlan.getSchema().value().size(), equalTo(4));
  }

  @Test
  public void testSimpleRightJoinFilterLogicalPlan() {
    final String
        simpleQuery =
        "SELECT t1.col1, t2.col1, col5, t2.col4, t2.col2 FROM test1 t1 RIGHT JOIN test2 t2 ON "
            + "t1.col0 = t2.col0 WHERE t1.col3 > 10.8 AND t2.col2 = 'foo' EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan.getSources().get(0), instanceOf(ProjectNode.class));
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    assertThat(projectNode.getSchema().value().size(), equalTo(5));

    assertThat(projectNode.getSources().get(0), instanceOf(FilterNode.class));
    final FilterNode filterNode = (FilterNode) projectNode.getSources().get(0);
    assertThat(filterNode.getPredicate().toString(), equalTo("((T1_COL3 > 10.8) AND (T2_COL2 = 'foo'))"));

    assertThat(filterNode.getSources().get(0), instanceOf(JoinNode.class));
    final JoinNode joinNode = (JoinNode) filterNode.getSources().get(0);
    final PlanNode leftSource = joinNode.getSources().get(0);
    assertThat(leftSource, instanceOf(ProjectNode.class));
    assertThat(leftSource.getSources().get(0), instanceOf(PreJoinRepartitionNode.class));
    final PlanNode rightSource = joinNode.getSources().get(0);
    assertThat(rightSource, instanceOf(ProjectNode.class));
    assertThat(rightSource.getSources().get(0), instanceOf(PreJoinRepartitionNode.class));
  }

  @Test
  public void testSuppressLogicalPlan() {
    final String simpleQuery = "SELECT col1,COUNT(*) as COUNT FROM test2 WINDOW TUMBLING (SIZE 2 MILLISECONDS, GRACE PERIOD 1 MILLISECONDS) GROUP BY col1 EMIT FINAL;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan.getSources().get(0), instanceOf(AggregateNode.class));
    assertThat(logicalPlan.getSources().get(0).getSources().get(0), instanceOf(DataSourceNode.class));
    assertThat(logicalPlan.getSchema().value().size(), equalTo( 2));
    assertThat(
        ((AggregateNode) logicalPlan.getSources().get(0)).getWindowExpression().get().getKsqlWindowExpression().getEmitStrategy().get(),
        equalTo(OutputRefinement.FINAL)
    );
  }

  private static SelectExpression selectCol(final String column, final String alias) {
    return SelectExpression.of(
        ColumnName.of(alias),
        new UnqualifiedColumnReferenceExp(ColumnName.of(column))
    );
  }

  @Test
  public void shouldAddProjectionWithSourceAliasPrefixForJoinSources() {
    // Given:
    final String simpleQuery = "SELECT t1.col1, t2.col1 FROM test1 t1 JOIN test2 t2 ON t1.col0 = t2.col0 EMIT CHANGES;";

    // When:
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    // Then:
    final JoinNode joinNode = (JoinNode) logicalPlan.getSources().get(0).getSources().get(0);
    final ProjectNode left = (ProjectNode) joinNode.getSources().get(0);
    assertThat(left.getSelectExpressions(), contains(
        selectCol("COL1", "T1_COL1"),
        selectCol("COL2", "T1_COL2"),
        selectCol("COL3", "T1_COL3"),
        selectCol("COL4", "T1_COL4"),
        selectCol("COL5", "T1_COL5"),
        selectCol("HEAD", "T1_HEAD"),
        selectCol("ROWTIME", "T1_ROWTIME"),
        selectCol("ROWPARTITION", "T1_ROWPARTITION"),
        selectCol("ROWOFFSET", "T1_ROWOFFSET"),
        selectCol("COL0", "T1_COL0")
    ));
    final ProjectNode right = (ProjectNode) joinNode.getSources().get(1);
    assertThat(right.getSelectExpressions(), contains(
        selectCol("COL1", "T2_COL1"),
        selectCol("COL2", "T2_COL2"),
        selectCol("COL3", "T2_COL3"),
        selectCol("COL4", "T2_COL4"),
        selectCol("ROWTIME", "T2_ROWTIME"),
        selectCol("ROWPARTITION", "T2_ROWPARTITION"),
        selectCol("ROWOFFSET", "T2_ROWOFFSET"),
        selectCol("COL0", "T2_COL0")
    ));
  }

  @Test
  public void shouldRewriteFinalSelectsForJoin() {
    // Given:
    final String simpleQuery = "SELECT t1.col1, t2.col1 FROM test1 t1 JOIN test2 t2 ON t1.col0 = t2.col0 EMIT CHANGES;";

    // When:
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    // Then:
    final ProjectNode project = (ProjectNode) logicalPlan.getSources().get(0);
    assertThat(project.getSelectExpressions(), contains(
        selectCol("T1_COL1", "T1_COL1"),
        selectCol("T2_COL1", "T2_COL1")
    ));
  }

  @Test
  public void shouldRewriteFilterForJoin() {
    // Given:
    final String simpleQuery = "SELECT t1.col1, t2.col1 FROM test1 t1 JOIN test2 t2 ON t1.col0 = t2.col0 WHERE t1.col1 = t2.col1 EMIT CHANGES;";

    // When:
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    // Then:
    final FilterNode filter = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);
    assertThat(filter.getPredicate(), equalTo(
        new ComparisonExpression(
            Type.EQUAL,
            new UnqualifiedColumnReferenceExp(ColumnName.of("T1_COL1")),
            new UnqualifiedColumnReferenceExp(ColumnName.of("T2_COL1")))
    ));
  }

  @Test
  public void shouldRewritePartitionByForJoin() {
    // Given:
    final String simpleQuery = "SELECT t1.col1, t2.col1 FROM test1 t1 JOIN test2 t2 ON t1.col0 = t2.col0 PARTITION BY t1.col1 EMIT CHANGES;";

    // When:
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    // Then:
    final UserRepartitionNode repart = (UserRepartitionNode) logicalPlan
        .getSources().get(0).getSources().get(0);

    assertThat(
        repart.getPartitionBys(),
        equalTo(ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("T1_COL1"))))
    );
  }

  @Test
  public void shouldCreateTableOutputForAggregateQuery() {
    final String simpleQuery = "SELECT col0, sum(floor(col3)*100)/count(col3) FROM test1 window "
        + "HOPPING ( size 2 second, advance by 1 second) "
        + "WHERE col0 > 100 GROUP BY col0 EMIT CHANGES;";

    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KTABLE));
  }

  @Test
  public void shouldCreateStreamOutputForStreamTableJoin() {
    final String
        simpleQuery =
        "SELECT t1.col1, t2.col1, col5, t2.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
            + "t1.col0 = t2.col0 WHERE t1.col3 > 10.8 AND t2.col2 = 'foo' EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KSTREAM));
  }

  @Test
  public void shouldCreateStreamOutputForStreamFilter() {
    final String
        simpleQuery = "SELECT * FROM test1 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KSTREAM));
  }

  @Test
  public void shouldCreateTableOutputForTableFilter() {
    final String
        simpleQuery = "SELECT * FROM test2 WHERE col2 = 'foo' EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KTABLE));
  }

  @Test
  public void shouldCreateStreamOutputForStreamProjection() {
    final String
        simpleQuery = "SELECT col0 FROM test1 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KSTREAM));
  }

  @Test
  public void shouldCreateTableOutputForTableProjection() {
    final String
        simpleQuery = "SELECT col4 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KTABLE));
  }

  @Test
  public void shouldCreateStreamOutputForStreamStreamJoin() {
    final String simpleQuery = "SELECT * FROM ORDERS INNER JOIN TEST1 ON ORDERS.ORDERID=TEST1.COL0 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KSTREAM));
  }

  @Test
  public void shouldCreateTableOutputForTableTableJoin() {
    final String simpleQuery = "SELECT * FROM TEST2 INNER JOIN TEST3 ON TEST2.COL0=TEST3.COL0 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KTABLE));
  }

  @Test
  public void shouldCreateTableOutputForTableSuppress() {
    final String simpleQuery = "SELECT col1,COUNT(*) as COUNT FROM test2 WINDOW TUMBLING (SIZE 2 MILLISECONDS, GRACE PERIOD 1 MILLISECONDS) GROUP BY col1 EMIT FINAL;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KTABLE));
  }

  @Test
  public void shouldThrowOnNonWindowedAggregationSuppressions() {
    final String simpleQuery = "SELECT * FROM test2 EMIT FINAL;";
    final Exception e = assertThrows(
        KsqlException.class,
        () -> buildLogicalPlan(simpleQuery)
    );

    assertThat(e.getMessage(), containsString("EMIT FINAL is only supported for windowed aggregations."));
  }

  @Test
  public void shouldThrowOnSuppressDisabledInConfig() {
    // Given:
    KsqlConfig ksqlConfigSuppressDisabled = new KsqlConfig(Collections.singletonMap(KsqlConfig.KSQL_SUPPRESS_ENABLED, false));
    final String simpleQuery = "SELECT col1,COUNT(*) as COUNT FROM test2 WINDOW TUMBLING (SIZE 2 MILLISECONDS, GRACE PERIOD 1 MILLISECONDS) GROUP BY col1 EMIT FINAL;";

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AnalysisTestUtil.buildLogicalPlan(ksqlConfigSuppressDisabled, simpleQuery, metaStore)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Suppression is currently disabled. You can enable it by setting ksql.suppress.enabled to true"));
  }

  @Test
  public void testLimitTableScanLogicalPlan() {
    final String simpleQuery = "SELECT * FROM test2 LIMIT 3;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan, instanceOf(KsqlBareOutputNode.class));
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KTABLE));

    final PlanNode finalProjectNode = logicalPlan.getSources().get(0);

    assertThat(finalProjectNode, instanceOf(FinalProjectNode.class));
    assertThat(finalProjectNode.getNodeOutputType(), equalTo(DataSourceType.KTABLE));

    final PlanNode queryLimitNode = finalProjectNode.getSources().get(0);

    assertThat(queryLimitNode, instanceOf(QueryLimitNode.class));
    assertThat(((QueryLimitNode) queryLimitNode).getLimit(), equalTo(3));
    assertThat(queryLimitNode.getNodeOutputType(), equalTo(DataSourceType.KTABLE));

    final PlanNode dataSourceNode = queryLimitNode.getSources().get(0);

    assertThat(dataSourceNode, instanceOf(DataSourceNode.class));
    assertThat(dataSourceNode.getNodeOutputType(), equalTo(DataSourceType.KTABLE));
  }

  @Test
  public void testLimitStreamPullQueryLogicalPlan() {
    final String simpleQuery = "SELECT * FROM test1 LIMIT 3;";
    final PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    assertThat(logicalPlan, instanceOf(KsqlBareOutputNode.class));
    assertThat(logicalPlan.getNodeOutputType(), equalTo(DataSourceType.KSTREAM));

    final PlanNode finalProjectNode = logicalPlan.getSources().get(0);

    assertThat(finalProjectNode, instanceOf(FinalProjectNode.class));
    assertThat(finalProjectNode.getNodeOutputType(), equalTo(DataSourceType.KSTREAM));

    final PlanNode queryLimitNode = finalProjectNode.getSources().get(0);

    assertThat(queryLimitNode, instanceOf(QueryLimitNode.class));
    assertThat(((QueryLimitNode) queryLimitNode).getLimit(), equalTo(3));
    assertThat(queryLimitNode.getNodeOutputType(), equalTo(DataSourceType.KSTREAM));

    final PlanNode dataSourceNode = queryLimitNode.getSources().get(0);

    assertThat(dataSourceNode, instanceOf(DataSourceNode.class));
    assertThat(dataSourceNode.getNodeOutputType(), equalTo(DataSourceType.KSTREAM));
  }

  private PlanNode buildLogicalPlan(final String query) {
    return AnalysisTestUtil.buildLogicalPlan(ksqlConfig, query, metaStore);
  }
}
