/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.physical.pull;

import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullPhysicalPlanType;
import io.confluent.ksql.query.PullQueryWriteStream;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KsqlConstants.QuerySourceType;
import io.confluent.ksql.util.KsqlConstants.RoutingNodeType;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullQueryResult {
  private static final Logger LOG = LoggerFactory.getLogger(PullQueryResult.class);

  private final LogicalSchema schema;
  private final PullQueryQueuePopulator populator;
  private final QueryId queryId;
  private final PullQueryWriteStream pullQueryQueue;
  private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;
  private final QuerySourceType sourceType;
  private final PullPhysicalPlanType planType;
  private final RoutingNodeType routingNodeType;
  private final Supplier<Long> rowsProcessedSupplier;
  private final CompletableFuture<Void> shouldCancelRequests;
  private final Optional<ConsistencyOffsetVector> consistencyOffsetVector;

  // This future is used to keep track of all of the callbacks since we allow for adding them both
  // before and after the pull query has been started.  When the pull query has completed, it will
  // pass on the outcome to this future.
  private CompletableFuture<Void> future = new CompletableFuture<>();
  private boolean started = false;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "ParameterNumber"})
  public PullQueryResult(
      final LogicalSchema schema,
      final PullQueryQueuePopulator populator,
      final QueryId queryId,
      final PullQueryWriteStream pullQueryQueue,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final QuerySourceType sourceType,
      final PullPhysicalPlanType planType,
      final RoutingNodeType routingNodeType,
      final Supplier<Long> rowsProcessedSupplier,
      final CompletableFuture<Void> shouldCancelRequests,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) {
    this.schema = schema;
    this.populator = populator;
    this.queryId = queryId;
    this.pullQueryQueue = pullQueryQueue;
    this.pullQueryMetrics = pullQueryMetrics;
    this.sourceType = sourceType;
    this.planType = planType;
    this.routingNodeType = routingNodeType;
    this.rowsProcessedSupplier = rowsProcessedSupplier;
    this.shouldCancelRequests = shouldCancelRequests;
    this.consistencyOffsetVector = Objects.requireNonNull(
        consistencyOffsetVector, "consistencyOffsetVector");
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public PullQueryWriteStream getPullQueryQueue() {
    return pullQueryQueue;
  }

  public Optional<ConsistencyOffsetVector> getConsistencyOffsetVector() {
    return consistencyOffsetVector;
  }

  public void start() {
    Preconditions.checkState(!started, "Should only start once");
    started = true;
    try {
      final CompletableFuture<Void> f = populator.run();
      f.exceptionally(t -> {
        future.completeExceptionally(t);
        return null;
      });
      f.thenAccept(future::complete);
    } catch (final Throwable t) {
      future.completeExceptionally(t);
      throw t;
    }
    // Register the error metric
    onException(t ->
        pullQueryMetrics.ifPresent(metrics ->
            metrics.recordErrorRate(1, sourceType, planType, routingNodeType))
    );
  }

  public void stop() {
    try {
      pullQueryQueue.end();
    } catch (final Throwable t) {
      LOG.error("Error closing pull query queue", t);
    }
    future.complete(null);
    shouldCancelRequests.complete(null);
  }

  public void onException(final Consumer<Throwable> consumer) {
    future.exceptionally(t -> {
      consumer.accept(t);
      return null;
    });
  }

  public void onCompletion(final Consumer<Void> consumer) {
    future.thenAccept(consumer);
  }

  public void onCompletionOrException(final BiConsumer<Void, Throwable> biConsumer) {
    future.handle((v, t) -> {
      biConsumer.accept(v, t);
      return null;
    });
  }

  public QuerySourceType getSourceType() {
    return sourceType;
  }

  public PullPhysicalPlanType getPlanType() {
    return planType;
  }

  public RoutingNodeType getRoutingNodeType() {
    return routingNodeType;
  }

  /**
   * @return Number of rows returned to user
   */
  public long getTotalRowsReturned() {
    return pullQueryQueue.getTotalRowsQueued();
  }

  /**
   * Number of rows read from the underlying data store. This does not need to match
   * the number of rows returned to the user as rows can get filtered out based on the
   * WHERE clause conditions.
   * @return Number of rows read from the data store
   */
  public long getTotalRowsProcessed() {
    return rowsProcessedSupplier.get();
  }
}
