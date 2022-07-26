package io.confluent.ksql.execution.scalablepush;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.execution.common.operators.ProjectOperator;
import io.confluent.ksql.execution.common.operators.SelectOperator;
import io.confluent.ksql.execution.scalablepush.operators.PeekStreamOperator;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.QueryFilterNode;
import io.confluent.ksql.planner.plan.QueryProjectNode;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.vertx.core.Context;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PushExecutionPlanBuilderTest {
  private static final String CONSUMER_GROUP = "cg";

  @Mock
  private ProcessingLogContext logContext;
  @Mock
  private ProcessingLoggerFactory processingLoggerFactory;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private PersistentQueryMetadata persistentQueryMetadata;
  @Mock
  private Context context;
  @Mock
  private LogicalPlanNode logicalPlanNode;
  @Mock
  private KsqlBareOutputNode ksqlBareOutputNode;
  @Mock
  private QueryProjectNode projectNode;
  @Mock
  private QueryFilterNode filterNode;
  @Mock
  private DataSourceNode dataSourceNode;
  @Mock
  private ScalablePushRegistry scalablePushRegistry;
  @Mock
  private Expression rewrittenExpression;
  @Mock
  private ExpressionEvaluator expressionEvaluator;
  @Mock
  private LogicalSchema logicalSchema;

  @Before
  public void setUp() {
    when(logContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger(any())).thenReturn(processingLogger);
    when(logicalPlanNode.getNode()).thenReturn(Optional.of(ksqlBareOutputNode));
    when(ksqlBareOutputNode.getSource()).thenReturn(projectNode);
    when(projectNode.getSources()).thenReturn(ImmutableList.of(filterNode));
    when(filterNode.getSources()).thenReturn(ImmutableList.of(dataSourceNode));
    when(persistentQueryMetadata.getScalablePushRegistry())
        .thenReturn(Optional.of(scalablePushRegistry));

    when(filterNode.getRewrittenPredicate()).thenReturn(rewrittenExpression);
    when(filterNode.getCompiledWhereClause()).thenReturn(expressionEvaluator);
    when(expressionEvaluator.getExpressionType()).thenReturn(SqlTypes.BOOLEAN);
    when(projectNode.getSchema()).thenReturn(logicalSchema);
    when(scalablePushRegistry.getCatchupConsumerId(any())).thenReturn(CONSUMER_GROUP);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBuildPhysicalPlan() {
    // Given:
    final PushPhysicalPlanBuilder builder = new PushPhysicalPlanBuilder(logContext,
        persistentQueryMetadata);

    // When:
    final PushPhysicalPlan pushPhysicalPlan =
        builder.buildPushPhysicalPlan(logicalPlanNode, context, Optional.empty(), Optional.empty());

    // Then:
    assertThat(pushPhysicalPlan.getRoot(), isA((Class) ProjectOperator.class));
    final ProjectOperator projectOperator = (ProjectOperator) pushPhysicalPlan.getRoot();
    assertThat(projectOperator.getChild(), isA((Class) SelectOperator.class));
    final SelectOperator selectOperator = (SelectOperator) projectOperator.getChild();
    assertThat(selectOperator.getChild(), isA((Class) PeekStreamOperator.class));
    assertThat(pushPhysicalPlan.getScalablePushRegistry(), is(scalablePushRegistry));
  }

  @Test
  public void shouldThrowOnNoOutputNode() {
    // Given:
    when(logicalPlanNode.getNode()).thenReturn(Optional.empty());
    final PushPhysicalPlanBuilder builder = new PushPhysicalPlanBuilder(logContext,
        persistentQueryMetadata);

    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> builder.buildPushPhysicalPlan(logicalPlanNode, context, Optional.empty(),
            Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Need an output node to build a plan"));
  }

  @Test
  public void shouldThrowOnNotBareOutputNode() {
    // Given:
    when(logicalPlanNode.getNode()).thenReturn(Optional.of(mock(OutputNode.class)));
    final PushPhysicalPlanBuilder builder = new PushPhysicalPlanBuilder(logContext,
        persistentQueryMetadata);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> builder.buildPushPhysicalPlan(logicalPlanNode, context, Optional.empty(),
            Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Push queries expect the root of the logical plan to be a "
            + "KsqlBareOutputNode."));
  }

  @Test
  public void shouldThrowOnUnknownLogicalNode() {
    // Given:
    when(ksqlBareOutputNode.getSource()).thenReturn(mock(PlanNode.class));
    final PushPhysicalPlanBuilder builder = new PushPhysicalPlanBuilder(logContext,
        persistentQueryMetadata);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> builder.buildPushPhysicalPlan(logicalPlanNode, context, Optional.empty(),
            Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), containsString("unrecognized logical node"));
  }

  @Test
  public void shouldThrowOnMultipleSources() {
    // Given:
    when(projectNode.getSources()).thenReturn(ImmutableList.of(filterNode, dataSourceNode));
    final PushPhysicalPlanBuilder builder = new PushPhysicalPlanBuilder(logContext,
        persistentQueryMetadata);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> builder.buildPushPhysicalPlan(logicalPlanNode, context, Optional.empty(),
            Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Push queries do not support joins or nested sub-queries yet"));
  }

  @Test
  public void shouldThrowOnNoDataSourceOperator() {
    // Given:
    when(filterNode.getSources()).thenReturn(ImmutableList.of());
    final PushPhysicalPlanBuilder builder = new PushPhysicalPlanBuilder(logContext,
        persistentQueryMetadata);

    // When:
    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> builder.buildPushPhysicalPlan(logicalPlanNode, context, Optional.empty(),
            Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("DataSourceOperator cannot be null in Push physical plan"));
  }
}
