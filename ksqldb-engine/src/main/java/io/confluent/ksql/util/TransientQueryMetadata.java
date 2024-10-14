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

package io.confluent.ksql.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.CompletionHandler;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.LimitHandler;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.streams.Topology;

/**
 * Metadata of a transient query, e.g. {@code SELECT * FROM FOO;}.
 */
public class TransientQueryMetadata extends QueryMetadataImpl implements PushQueryMetadata {

  private final BlockingRowQueue rowQueue;
  private final ResultType resultType;
  final AtomicBoolean isRunning = new AtomicBoolean(true);

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public TransientQueryMetadata(
      final String statementString,
      final LogicalSchema logicalSchema,
      final Set<SourceName> sourceNames,
      final String executionPlan,
      final BlockingRowQueue rowQueue,
      final QueryId queryId,
      final String queryApplicationId,
      final Topology topology,
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final Map<String, Object> streamsProperties,
      final Map<String, Object> overriddenProperties,
      final long closeTimeout,
      final int maxQueryErrorsQueueSize,
      final ResultType resultType,
      final long retryBackoffInitialMs,
      final long retryBackoffMaxMs,
      final Listener listener
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    super(
        statementString,
        logicalSchema,
        sourceNames,
        executionPlan,
        queryApplicationId,
        topology,
        kafkaStreamsBuilder,
        streamsProperties,
        overriddenProperties,
        closeTimeout,
        queryId,
        QueryErrorClassifier.DEFAULT_CLASSIFIER,
        maxQueryErrorsQueueSize,
        retryBackoffInitialMs,
        retryBackoffMaxMs,
        listener
    );
    this.rowQueue = Objects.requireNonNull(rowQueue, "rowQueue");
    this.resultType = Objects.requireNonNull(resultType, "resultType");
  }

  public TransientQueryMetadata(
      final TransientQueryMetadata original,
      final BlockingRowQueue rowQueue,
      final Listener listener
  ) {
    super(original, listener);
    this.rowQueue = Objects.requireNonNull(rowQueue, "rowQueue");
    this.resultType = original.resultType;
  }

  public boolean isRunning() {
    return isRunning.get() && getKafkaStreams().state().isRunningOrRebalancing();
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public BlockingRowQueue getRowQueue() {
    return rowQueue;
  }

  @Override
  public KsqlQueryType getQueryType() {
    return KsqlQueryType.PUSH;
  }

  public ResultType getResultType() {
    return resultType;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof TransientQueryMetadata)) {
      return false;
    }

    final TransientQueryMetadata that = (TransientQueryMetadata) o;

    return Objects.equals(this.rowQueue, that.rowQueue)
        && Objects.equals(this.resultType, that.resultType)
        && super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowQueue, resultType, super.hashCode());
  }

  public void setLimitHandler(final LimitHandler limitHandler) {
    rowQueue.setLimitHandler(limitHandler);
  }

  @Override
  public void setCompletionHandler(final CompletionHandler completionHandler) {
    rowQueue.setCompletionHandler(completionHandler);
  }

  @Override
  public void close() {
    // Push queries can be closed by both terminate commands and the client ending the request, so
    // we ensure that there's no race and that close is called just once.
    if (!this.isRunning.compareAndSet(true, false)) {
      return;
    }
    // To avoid deadlock, close the queue first to ensure producer side isn't blocked trying to
    // write to the blocking queue, otherwise super.close call can deadlock:
    rowQueue.close();

    // Now safe to close:
    super.close();
  }
}
