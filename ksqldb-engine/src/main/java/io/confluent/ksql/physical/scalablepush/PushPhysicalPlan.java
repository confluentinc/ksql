/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.physical.scalablepush;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.physical.common.operators.AbstractPhysicalOperator;
import io.confluent.ksql.physical.pull.PullPhysicalPlan;
import io.confluent.ksql.physical.scalablepush.operators.PushDataSourceOperator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a physical plan for a scalable push query. The execution of the plan is done async
 * on a {@link Context} so that running queries don't need to use dedicated threads. This is
 * especially important since push queries tend to be long-running. This should allow scalable push
 * queries to have many concurrent requests.
 */
public class PushPhysicalPlan {
  // Subscribers always demand one row, so it shouldn't be that any buffering is required in
  // practice.
  private static final int CAPACITY = Integer.MAX_VALUE;

  private static final Logger LOGGER = LoggerFactory.getLogger(PullPhysicalPlan.class);

  private final AbstractPhysicalOperator root;

  private final LogicalSchema schema;
  private final QueryId queryId;
  private final ScalablePushRegistry scalablePushRegistry;
  private final PushDataSourceOperator dataSourceOperator;
  private final Context context;
  private volatile boolean closed = false;
  private long timer = -1;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
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
    final Publisher publisher = new Publisher(context);
    context.runOnContext(v -> open(publisher));
    return publisher;
  }

  public boolean isClosed() {
    return closed;
  }

  private void maybeNext(final Publisher publisher) {
    List<?> row;
    while ((row = (List<?>)next()) != null) {
      if (dataSourceOperator.droppedRows()) {
        closeInternal();
        publisher.reportDroppedRows();
        break;
      } else {
        publisher.accept(row);
      }
    }
    if (!closed) {
      if (timer >= 0) {
        context.owner().cancelTimer(timer);
      }
      // Schedule another batch async
      timer = context.owner()
          .setTimer(100, timerId -> context.runOnContext(v -> maybeNext(publisher)));
    } else {
      publisher.close();
    }
  }

  private void open(final Publisher publisher) {
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

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
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

  public static class Publisher extends BufferedPublisher<List<?>> {

    public Publisher(final Context ctx) {
      super(ctx, CAPACITY);
    }

    public void reportDroppedRows() {
      sendError(new RuntimeException("Dropped rows"));
    }
  }
}
