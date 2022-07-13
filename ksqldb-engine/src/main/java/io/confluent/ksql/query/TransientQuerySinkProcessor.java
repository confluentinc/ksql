/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.vertx.core.impl.ConcurrentHashSet;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.StreamTask;

final class TransientQuerySinkProcessor implements Processor<Object, GenericRow, Void, Void> {

  private final TransientQueryQueue queue;
  private final Optional<ImmutableMap<TopicPartition, Long>> endOffsets;
  private final ConcurrentHashSet<TopicPartition> donePartitions;
  private ProcessorContext<Void, Void> context;

  public static ProcessorSupplier<Object, GenericRow, Void, Void> supplier(
      final TransientQueryQueue queue,
      final Optional<ImmutableMap<TopicPartition, Long>> endOffsets,
      final ConcurrentHashSet<TopicPartition> donePartitions) {
    return () -> new TransientQuerySinkProcessor(queue, endOffsets, donePartitions);
  }

  private TransientQuerySinkProcessor(
      final TransientQueryQueue queue,
      final Optional<ImmutableMap<TopicPartition, Long>> endOffsets,
      final ConcurrentHashSet<TopicPartition> donePartitions) {

    this.queue = queue;
    this.endOffsets = endOffsets;
    this.donePartitions = donePartitions;
  }

  @Override
  public void init(final ProcessorContext<Void, Void> context) {
    Processor.super.init(context);
    this.context = context;
    if (endOffsets.isPresent()) {
      // registering a punctuation here is the easiest way to check
      // if we're complete even if we don't see all records.
      // The in-line check in process() is faster,
      // but some records may be filtered out before reaching us.
      context.schedule(
          Duration.ofMillis(100),
          PunctuationType.WALL_CLOCK_TIME,
          timestamp -> checkForQueryCompletion()
      );
    }
  }

  @Override
  public void process(final Record<Object, GenericRow> record) {
    final Optional<TopicPartition> topicPartition =
        context
            .recordMetadata()
            .map(m -> new TopicPartition(m.topic(), m.partition()));

    final boolean alreadyDone =
        topicPartition.isPresent() && donePartitions.contains(topicPartition.get());

    // We ignore null values in streams as invalid data and drop the record.
    // If we're already done with the current partition, we just drop the record.
    if (record.value() != null && !alreadyDone) {
      queue.acceptRow(null, record.value());
    }

    topicPartition.ifPresent(this::checkForPartitionCompletion);
  }

  private void checkForPartitionCompletion(final TopicPartition topicPartition) {
    if (endOffsets.isPresent()) {
      final ImmutableMap<TopicPartition, Long> endOffsetsMap = endOffsets.get();
      if (endOffsetsMap.containsKey(topicPartition)) {
        final Map<TopicPartition, OffsetAndMetadata> currentPositions = getCurrentPositions();
        checkCompletion(topicPartition, currentPositions, endOffsetsMap.get(topicPartition));
      }
    }
  }

  private void checkForQueryCompletion() {
    if (endOffsets.isPresent()) {
      final Map<TopicPartition, OffsetAndMetadata> currentPositions =
          getCurrentPositions();

      for (final Entry<TopicPartition, Long> end : endOffsets.get().entrySet()) {
        checkCompletion(
            end.getKey(),
            currentPositions,
            end.getValue()
        );
      }
      // if we're completely done with this query, then call the completion handler.
      if (ImmutableSet.copyOf(donePartitions).equals(endOffsets.get().keySet())) {
        queue.complete();
      }
    }
  }

  private void checkCompletion(final TopicPartition topicPartition,
      final Map<TopicPartition, OffsetAndMetadata> currentPositions,
      final Long end) {

    if (currentPositions.containsKey(topicPartition)) {
      final OffsetAndMetadata current = currentPositions.get(topicPartition);
      if (current != null && current.offset() >= end) {
        donePartitions.add(topicPartition);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Map<TopicPartition, OffsetAndMetadata> getCurrentPositions() {
    // It might make sense to actually propose adding the concept of getting the
    // "current position" in the ProcessorContext, but for now we just expose it
    // by reflection. Obviously, this code is pretty brittle.
    try {
      if (context.getClass().equals(ProcessorContextImpl.class)) {
        final Field streamTask;
        streamTask = ProcessorContextImpl.class.getDeclaredField("streamTask");
        streamTask.setAccessible(true);
        final StreamTask task = (StreamTask) streamTask.get(context);
        final Method committableOffsetsAndMetadata =
            StreamTask.class.getDeclaredMethod("committableOffsetsAndMetadata");
        committableOffsetsAndMetadata.setAccessible(true);
        return (Map<TopicPartition, OffsetAndMetadata>)
            committableOffsetsAndMetadata.invoke(task);
      } else {
        // Specifically, this will break if you try to run this processor in a
        // unit test with a mock processor. Both KafkaStreams and the TopologyTestDriver
        // use a ProcessorContextImpl, which should be the only way this processor gets run.
        throw new IllegalStateException(
            "Expected only to run in the KafkaStreams or TopologyTestDriver runtimes."
        );
      }
    } catch (final NoSuchFieldException | IllegalAccessException
        | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

}
