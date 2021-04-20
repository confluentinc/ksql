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
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushPhysicalPlan {
  private static final int CAPACITY = Integer.MAX_VALUE;

  private static final Logger LOGGER = LoggerFactory.getLogger(PullPhysicalPlan.class);

  private final AbstractPhysicalOperator root;

  private final LogicalSchema schema;
  private final QueryId queryId;
  private final ScalablePushRegistry scalablePushRegistry;
  private final PushDataSourceOperator dataSourceOperator;
  private final Context context;
  private boolean closed = false;
  private long timer = -1;

  public PushPhysicalPlan(
      final AbstractPhysicalOperator root,
      final LogicalSchema schema,
      final QueryId queryId,
      final ScalablePushRegistry scalablePushRegistry,
      final PushDataSourceOperator dataSourceOperator,
      final Context context
  ) {
    this.root = Objects.requireNonNull(root, "root");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.scalablePushRegistry =
        Objects.requireNonNull(scalablePushRegistry, "scalablePushRegistry");
    this.dataSourceOperator = dataSourceOperator;
    this.context = context;
  }

  public BufferedPublisher<List<?>> execute() {
    final BufferedPublisher<List<?>> publisher = new BufferedPublisher<>(context, CAPACITY);
    context.runOnContext(v -> open(publisher));
    return publisher;
  }

  private void maybeNext(final BufferedPublisher<List<?>> publisher) {
    List<?> row;
    while ((row = (List<?>)next()) != null) {
      if (publisher.accept(row)) {
        break;
      }
    }
    if (!closed) {
      if (timer >= 0) {
        context.owner().cancelTimer(timer);
      }
      // Schedule another batch async
      timer = context.owner()
          .setTimer(100, timerId -> context.runOnContext(v -> maybeNext(publisher)));
    }
  }

  private void open(final BufferedPublisher<List<?>> publisher) {
    VertxUtils.checkContext(context);
    dataSourceOperator.setNewRowCallback(() -> context.runOnContext(v -> maybeNext(publisher)));
    root.open();
    maybeNext(publisher);
  }

  private Object next() {
    VertxUtils.checkContext(context);
    return root.next();
  }

  public void close() {
    context.runOnContext(v -> closeInternal());
  }

  private void closeInternal() {
    VertxUtils.checkContext(context);
    closed = true;
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
