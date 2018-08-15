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

package io.confluent.ksql.planner.plan;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.connect.data.Schema;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Immutable
public abstract class OutputNode
    extends PlanNode {

  private final PlanNode source;
  private final Schema schema;
  private final Optional<Integer> limit;
  private final TimestampExtractionPolicy timestampExtractionPolicy;
  private final InternalCallback callback;

  public interface LimitHandler {
    void limitReached();
  }

  public interface Callback {

    /**
     * Called to determine is an output row should be queued for output.
     *
     * @return {@code true} if it should be sent, {@code false} otherwise.
     */
    boolean shouldQueue();

    /**
     * Called once a row has been queued for output.
     */
    void onQueued();
  }

  @JsonCreator
  protected OutputNode(
      @JsonProperty("id") final PlanNodeId id,
      @JsonProperty("source") final PlanNode source,
      @JsonProperty("schema") final Schema schema,
      @JsonProperty("limit") final Optional<Integer> limit,
      @JsonProperty("timestamp_policy") final TimestampExtractionPolicy timestampExtractionPolicy) {
    super(id);
    requireNonNull(source, "source is null");
    requireNonNull(schema, "schema is null");
    requireNonNull(timestampExtractionPolicy, "timestampExtractionPolicy is null");

    this.source = source;
    this.schema = schema;
    this.limit = limit;
    this.timestampExtractionPolicy = timestampExtractionPolicy;
    this.callback = limit
        .map(l -> (InternalCallback) new LimitCallback(l))
        .orElseGet(NoCallback::new);
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  public Optional<Integer> getLimit() {
    return limit;
  }

  /**
   * @return a callback to be called before outputting.
   */
  public Callback getCallback() {
    return callback;
  }

  public void setLimitHandler(final LimitHandler limitHandler) {
    callback.setLimitHandler(limitHandler);
  }

  @JsonProperty
  public PlanNode getSource() {
    return source;
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitOutput(this, context);
  }

  public TimestampExtractionPolicy getTimestampExtractionPolicy() {
    return timestampExtractionPolicy;
  }

  public TimestampExtractionPolicy getSourceTimestampExtractionPolicy() {
    return source.getTheSourceNode().getTimestampExtractionPolicy();
  }

  private interface InternalCallback extends Callback {

    void setLimitHandler(LimitHandler limitHandler);
  }

  private static class LimitCallback implements InternalCallback {

    private final AtomicInteger remaining;
    private final AtomicInteger queued;
    private volatile LimitHandler limitHandler = () -> {
    };

    private LimitCallback(final int limit) {
      if (limit <= 0) {
        throw new IllegalArgumentException("limit must be positive, was:" + limit);
      }
      this.remaining = new AtomicInteger(limit);
      this.queued = new AtomicInteger(limit);
    }

    @Override
    public void setLimitHandler(final LimitHandler limitHandler) {
      this.limitHandler = Objects.requireNonNull(limitHandler, "limitHandler");
    }

    @Override
    public boolean shouldQueue() {
      return remaining.decrementAndGet() >= 0;
    }

    @Override
    public void onQueued() {
      if (queued.decrementAndGet() == 0) {
        limitHandler.limitReached();
      }
    }
  }

  private static class NoCallback implements InternalCallback {

    @Override
    public void setLimitHandler(final LimitHandler limitHandler) {
    }

    @Override
    public boolean shouldQueue() {
      return true;
    }

    @Override
    public void onQueued() {
    }
  }
}
