package io.confluent.ksql.physical.scalable_push;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlKey;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.physical.pull.PullPhysicalPlan;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullPhysicalPlanType;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullSourceType;
import io.confluent.ksql.physical.pull.PullQueryRow;
import io.confluent.ksql.physical.pull.operators.AbstractPhysicalOperator;
import io.confluent.ksql.physical.pull.operators.DataSourceOperator;
import io.confluent.ksql.physical.scalable_push.operators.PushDataSourceOperator;
import io.confluent.ksql.planner.plan.KeyConstraint;
import io.confluent.ksql.planner.plan.KeyConstraint.ConstraintOperator;
import io.confluent.ksql.planner.plan.LookupConstraint;
import io.confluent.ksql.query.PullQueryQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushPhysicalPlan {

  private static final Logger LOGGER = LoggerFactory.getLogger(PullPhysicalPlan.class);

  private final AbstractPhysicalOperator root;

  private final LogicalSchema schema;
  private final QueryId queryId;
  private final ScalablePushRegistry scalablePushRegistry;
  private final PushDataSourceOperator dataSourceOperator;

  public PushPhysicalPlan(
      final AbstractPhysicalOperator root,
      final LogicalSchema schema,
      final QueryId queryId,
      final ScalablePushRegistry scalablePushRegistry,
      final PushDataSourceOperator dataSourceOperator
  ) {
    this.root = Objects.requireNonNull(root, "root");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.scalablePushRegistry =
        Objects.requireNonNull(scalablePushRegistry, "scalablePushRegistry");
    this.dataSourceOperator = dataSourceOperator;
  }

  public void execute(
      final TransientQueryQueue transientQueryQueue) {

    open();
    List<?> row;
    while ((row = (List<?>)next()) != null) {
      if (transientQueryQueue.isClosed()) {
        break;
      }
      transientQueryQueue.acceptRow(null, GenericRow.fromList(row));
    }
    close();
  }

  private void open() {
    root.open();
  }

  private Object next() {
    return root.next();
  }

  public void close() {
    root.close();
  }

  public AbstractPhysicalOperator getRoot() {
    return root;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public LogicalSchema getOutputSchema() {
    return schema;
  }

  public ScalablePushRegistry getScalablePushRegistry() {
    return scalablePushRegistry;
  }
}
