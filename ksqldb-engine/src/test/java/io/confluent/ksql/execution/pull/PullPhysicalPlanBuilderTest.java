package io.confluent.ksql.execution.pull;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.QueryPlannerOptions;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.KeyConstraint;
import io.confluent.ksql.planner.plan.KeyConstraint.ConstraintOperator;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.QueryFilterNode;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PullPhysicalPlanBuilderTest {

  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private PersistentQueryMetadata persistentQueryMetadata;
  @Mock
  private ImmutableAnalysis immutableAnalysis;
  @Mock
  private QueryPlannerOptions queryPlannerOptions;
  @Mock
  private LogicalPlanNode logicalPlanNode;
  @Mock
  private KsqlBareOutputNode outputNode;
  @Mock
  private DataSourceNode dataSourceNode;
  @Mock
  private QueryFilterNode queryFilterNode;
  @Mock
  private Materialization materialization;
  @Mock
  private ProcessingLoggerFactory processingLoggerFactory;
  @Mock
  private ProcessingLogger processingLogger;

  private CompletableFuture<Void> cancel = new CompletableFuture<>();

  private PullPhysicalPlanBuilder builder;

  @Before
  public void setUp() {
    when(persistentQueryMetadata.getMaterialization(any(), any()))
        .thenReturn(Optional.of(materialization));
    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger(any())).thenReturn(processingLogger);
    when(queryPlannerOptions.getTableScansEnabled()).thenReturn(true);

    // Setup query filter node
    when(queryFilterNode.getRewrittenPredicate()).thenReturn(mock(Expression.class));
    ExpressionEvaluator evaluator = mock(ExpressionEvaluator.class);
    when(evaluator.getExpressionType()).thenReturn(SqlTypes.BOOLEAN);
    when(queryFilterNode.getCompiledWhereClause()).thenReturn(evaluator);
    LogicalSchema logicalSchema = mock(LogicalSchema.class);
    when(queryFilterNode.getSchema()).thenReturn(logicalSchema);

    // Chain of sources
    when(logicalPlanNode.getNode()).thenReturn(Optional.of(outputNode));
    when(outputNode.getSource()).thenReturn(queryFilterNode);
    when(queryFilterNode.getSources()).thenReturn(ImmutableList.of(dataSourceNode));

    // Build
    builder = new PullPhysicalPlanBuilder(
        processingLogContext, persistentQueryMetadata, immutableAnalysis, queryPlannerOptions,
        cancel, Optional.empty());
  }

  @Test
  public void shouldBuildPlan_singleKeyFilter() {
    // Given:
    when(queryFilterNode.getLookupConstraints()).thenReturn(ImmutableList.of(new KeyConstraint(
        ConstraintOperator.EQUAL, GenericKey.fromList(ImmutableList.of(1)), Optional.empty())));

    // When:
    PullPhysicalPlan pullPhysicalPlan = builder.buildPullPhysicalPlan(logicalPlanNode);

    // Then:
    assertThat(pullPhysicalPlan.getKeys().size(), is(1));
  }

  @Test
  public void shouldBuildPlan_singleMultiColumnKey() {
    // Given:
    when(queryFilterNode.getLookupConstraints()).thenReturn(ImmutableList.of(new KeyConstraint(
        ConstraintOperator.EQUAL, GenericKey.fromList(ImmutableList.of(1, 2)), Optional.empty())));

    // When:
    PullPhysicalPlan pullPhysicalPlan = builder.buildPullPhysicalPlan(logicalPlanNode);

    // Then:
    assertThat(pullPhysicalPlan.getKeys().size(), is(0));
  }

  @Test
  public void shouldBuildPlan_multipleKeys() {
    // Given:
    when(queryFilterNode.getLookupConstraints()).thenReturn(ImmutableList.of(
        new KeyConstraint(
            ConstraintOperator.EQUAL, GenericKey.fromList(ImmutableList.of(1)), Optional.empty()),
        new KeyConstraint(
            ConstraintOperator.EQUAL, GenericKey.fromList(ImmutableList.of(2)), Optional.empty())));

    // When:
    PullPhysicalPlan pullPhysicalPlan = builder.buildPullPhysicalPlan(logicalPlanNode);

    // Then:
    assertThat(pullPhysicalPlan.getKeys().size(), is(2));
  }

  @Test
  public void shouldBuildPlan_singleMultiColumnKey_scansDisabled() {
    // Given:
    when(queryFilterNode.getLookupConstraints()).thenReturn(ImmutableList.of(new KeyConstraint(
        ConstraintOperator.EQUAL, GenericKey.fromList(ImmutableList.of(1, 2)), Optional.empty())));
    when(queryPlannerOptions.getTableScansEnabled()).thenReturn(false);

    // When:
    PullPhysicalPlan pullPhysicalPlan = builder.buildPullPhysicalPlan(logicalPlanNode);

    // Then:
    assertThat(pullPhysicalPlan.getKeys().size(), is(1));
  }
}
