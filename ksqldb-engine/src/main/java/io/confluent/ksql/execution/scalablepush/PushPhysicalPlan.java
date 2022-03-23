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

package io.confluent.ksql.execution.scalablepush;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.execution.common.operators.AbstractPhysicalOperator;
import io.confluent.ksql.execution.pull.PullPhysicalPlan;
import io.confluent.ksql.execution.scalablepush.operators.PushDataSourceOperator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConstants.QuerySourceType;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import java.util.Objects;
import java.util.Optional;
import org.reactivestreams.Subscriber;
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
  private final String catchupConsumerGroupId;
  private final ScalablePushRegistry scalablePushRegistry;
  private final PushDataSourceOperator dataSourceOperator;
  private final Context context;
  private final QuerySourceType querySourceType;
  private volatile boolean closed = false;
  private long timer = -1;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public PushPhysicalPlan(
      final AbstractPhysicalOperator root,
      final LogicalSchema schema,
      final QueryId queryId,
      final String catchupConsumerGroupId,
      final ScalablePushRegistry scalablePushRegistry,
      final PushDataSourceOperator dataSourceOperator,
      final Context context,
      final QuerySourceType querySourceType
  ) {
    this.root = Objects.requireNonNull(root, "root");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.catchupConsumerGroupId =
        Objects.requireNonNull(catchupConsumerGroupId, "catchupConsumerGroupId");
    this.scalablePushRegistry =
        Objects.requireNonNull(scalablePushRegistry, "scalablePushRegistry");
    this.dataSourceOperator = dataSourceOperator;
    this.context = Objects.requireNonNull(context, "context");
    this.querySourceType = Objects.requireNonNull(querySourceType, "querySourceType");
  }

  public BufferedPublisher<QueryRow> execute() {
    return subscribeAndExecute(Optional.empty());
  }

  // for testing only
  BufferedPublisher<QueryRow> subscribeAndExecute(final Optional<Subscriber<QueryRow>> subscriber) {
    final Publisher publisher = new Publisher(context);
    subscriber.ifPresent(publisher::subscribe);
    context.runOnContext(v -> open(publisher));
    return publisher;
  }

  public boolean isClosed() {
    return closed;
  }

  private void maybeNext(final Publisher publisher) {
    QueryRow row;
    while (!isErrored(publisher) && (row = (QueryRow) next(publisher)) != null) {
      publisher.accept(row);
    }
    if (publisher.isFailed()) {
      return;
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

  private boolean isErrored(final Publisher publisher) {
    if (dataSourceOperator.droppedRows()) {
      closeInternal();
      publisher.reportDroppedRows();
      return true;
    } else if (dataSourceOperator.hasError()) {
      closeInternal();
      publisher.reportHasError();
      return true;
    }
    return false;
  }

  private void open(final Publisher publisher) {
    VertxUtils.checkContext(context);
    try {
      dataSourceOperator.setNewRowCallback(() -> context.runOnContext(v -> maybeNext(publisher)));
      root.open();
      maybeNext(publisher);
    } catch (Throwable t) {
      publisher.sendException(t);
    }
  }

  private Object next(final Publisher publisher) {
    VertxUtils.checkContext(context);
    try {
      return root.next();
    } catch (final Throwable t) {
      publisher.sendException(t);
      return null;
    }
  }

  public void close() {
    context.runOnContext(v -> closeInternal());
  }

  private void closeInternal() {
    VertxUtils.checkContext(context);
    if (!closed) {
      closed = true;
      root.close();
    }
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

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public ScalablePushRegistry getScalablePushRegistry() {
    return scalablePushRegistry;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public Context getContext() {
    return context;
  }

  public QuerySourceType getSourceType() {
    return querySourceType;
  }

  public long getRowsReadFromDataSource() {
    return dataSourceOperator.getRowsReadCount();
  }

  public String getCatchupConsumerGroupId() {
    return catchupConsumerGroupId;
  }

  public static class Publisher extends BufferedPublisher<QueryRow> {

    public Publisher(final Context ctx) {
      super(ctx, CAPACITY);
    }

    public void reportDroppedRows() {
      sendError(new RuntimeException("Dropped rows"));
    }

    public void reportHasError() {
      sendError(new RuntimeException("Internal error occurred"));
    }

    public void sendException(final Throwable e) {
      sendError(e);
    }

    public boolean isFailed() {
      return super.isFailed();
    }
  }
}
