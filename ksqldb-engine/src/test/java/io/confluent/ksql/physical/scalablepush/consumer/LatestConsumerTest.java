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

package io.confluent.ksql.physical.scalablepush.consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Clock;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LatestConsumerTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
      .build();

  private static final String TOPIC = "topic";
  private static final int NUM_PARTITIONS = 2;
  private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);
  private static final long CURRENT_TIME_MS = 60000;

  private static final ConsumerRecord<GenericKey, GenericRow> RECORD0_0
      = createRecord(0, 0, 0L,
      GenericKey.fromList(ImmutableList.of("k00")), GenericRow.fromList(ImmutableList.of(0)));
  private static final ConsumerRecord<GenericKey, GenericRow> RECORD0_1
      = createRecord(0, 1, 1L,
      GenericKey.fromList(ImmutableList.of("k01")), GenericRow.fromList(ImmutableList.of(1)));
  private static final ConsumerRecord<GenericKey, GenericRow> RECORD0_2
      = createRecord(0, 2, 2L,
      GenericKey.fromList(ImmutableList.of("k02")), GenericRow.fromList(ImmutableList.of(2)));
  private static final ConsumerRecord<GenericKey, GenericRow> RECORD0_3
      = createRecord(0, 3, 3L,
      GenericKey.fromList(ImmutableList.of("k03")), GenericRow.fromList(ImmutableList.of(3)));
  private static final ConsumerRecord<GenericKey, GenericRow> RECORD1_0
      = createRecord(1, 0, 4L,
      GenericKey.fromList(ImmutableList.of("k10")), GenericRow.fromList(ImmutableList.of(10)));
  private static final ConsumerRecord<GenericKey, GenericRow> RECORD1_1
      = createRecord(1, 1, 5L,
      GenericKey.fromList(ImmutableList.of("k11")), GenericRow.fromList(ImmutableList.of(11)));
  private static final ConsumerRecord<GenericKey, GenericRow> RECORD1_2
      = createRecord(1, 2, 6L,
      GenericKey.fromList(ImmutableList.of("k12")), GenericRow.fromList(ImmutableList.of(12)));
  private static final ConsumerRecord<GenericKey, GenericRow> RECORD1_3
      = createRecord(1, 4, 7L,
      GenericKey.fromList(ImmutableList.of("k13")), GenericRow.fromList(ImmutableList.of(13)));

  ConsumerRecords<GenericKey, GenericRow> RECORDS1 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(RECORD0_0),
          TP1, ImmutableList.of(RECORD1_0))
  );
  ConsumerRecords<GenericKey, GenericRow> RECORDS2 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(RECORD0_1),
          TP1, ImmutableList.of(RECORD1_1))
  );
  ConsumerRecords<GenericKey, GenericRow> RECORDS_FROM_OFFSET2 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(RECORD0_2, RECORD0_3),
          TP1, ImmutableList.of(RECORD1_2, RECORD1_3))
  );
  ConsumerRecords<GenericKey, GenericRow> EMPTY_RECORDS = new ConsumerRecords<>(
      ImmutableMap.of());


  @Mock
  private KafkaConsumer<Object, GenericRow> kafkaConsumer;
  @Mock
  private ProcessingQueue queue;
  @Mock
  private java.util.function.Consumer<Collection<TopicPartition>> catchupAssignmentUpdater;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private Clock clock;

  @Before
  public void setUp() {
    when(queue.getQueryId()).thenReturn(new QueryId("a"));
    when(clock.millis()).thenReturn(CURRENT_TIME_MS);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRunConsumer_success() {
    try (LatestConsumer consumer = spy(new LatestConsumer(
        NUM_PARTITIONS, TOPIC, false, SCHEMA, kafkaConsumer, new NoopCatchupCoordinator(),
        catchupAssignmentUpdater, ksqlConfig, clock))) {
      AtomicInteger count = new AtomicInteger(0);

      doAnswer(a -> {
        ConsumerRebalanceListener listener = a.getArgument(1);
        listener.onPartitionsAssigned(ImmutableList.of(TP0, TP1));
        return null;
      }).when(kafkaConsumer).subscribe(any(Collection.class), any());
      when(kafkaConsumer.poll(any())).thenAnswer(
          a -> {
            count.incrementAndGet();
            if (count.get() == 1) {
              return RECORDS1;
            } else if (count.get() == 2) {
              return RECORDS2;
            } else {
              consumer.close();
              return EMPTY_RECORDS;
            }
          });
      when(kafkaConsumer.position(TP0)).thenReturn(0L, 1L, 2L);
      when(kafkaConsumer.position(TP1)).thenReturn(0L, 1L, 2L);

      consumer.register(queue);
      consumer.run();

      verifyRows(
          ImmutableList.of(RECORD0_0, RECORD1_0, RECORD0_1, RECORD1_1));
      verify(consumer, times(1)).initialize();
      verify(consumer, times(1)).onNewAssignment();
      verify(consumer, times(2)).afterCommit();
      verify(consumer, times(1)).onEmptyRecords();
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 2L, TP1, 2L)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRunConsumer_seekToEnd() {
    try (LatestConsumer consumer = spy(new LatestConsumer(
        NUM_PARTITIONS, TOPIC, false, SCHEMA, kafkaConsumer, new NoopCatchupCoordinator(),
        catchupAssignmentUpdater, ksqlConfig, clock))) {
      AtomicInteger count = new AtomicInteger(0);

      doAnswer(a -> {
        ConsumerRebalanceListener listener = a.getArgument(1);
        listener.onPartitionsAssigned(ImmutableList.of(TP0, TP1));
        return null;
      }).when(kafkaConsumer).subscribe(any(Collection.class), any());
      when(kafkaConsumer.poll(any())).thenAnswer(
          a -> {
            count.incrementAndGet();
            if (count.get() == 1) {
              return RECORDS_FROM_OFFSET2;
            } else {
              consumer.close();
              return EMPTY_RECORDS;
            }
          });
      when(kafkaConsumer.position(TP0)).thenReturn(1L, 4L);
      when(kafkaConsumer.position(TP1)).thenReturn(1L, 4L);
      when(kafkaConsumer.offsetsForTimes(ImmutableMap.of(TP0, 30000L, TP1, 30000L))).thenReturn(
          ImmutableMap.of(TP0, new OffsetAndTimestamp(2L, 30000L),
              TP1, new OffsetAndTimestamp(2L, 30000L))
      );
      when(kafkaConsumer.committed(ImmutableSet.of(TP0, TP1))).thenReturn(
          ImmutableMap.of(TP0, new OffsetAndMetadata(1L),
              TP1, new OffsetAndMetadata(1L))
      );

      consumer.register(queue);
      consumer.run();

      verify(kafkaConsumer).seekToEnd(ImmutableSet.of(TP0, TP1));
      verifyRows(
          ImmutableList.of(RECORD0_2, RECORD0_3, RECORD1_2, RECORD1_3));
      verify(consumer, times(1)).initialize();
      verify(consumer, times(1)).onNewAssignment();
      verify(consumer, times(1)).afterCommit();
      verify(consumer, times(1)).onEmptyRecords();
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 4L, TP1, 4L)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRunConsumer_dontSeekToEnd() {
    try (LatestConsumer consumer = spy(new LatestConsumer(
        NUM_PARTITIONS, TOPIC, false, SCHEMA, kafkaConsumer, new NoopCatchupCoordinator(),
        catchupAssignmentUpdater, ksqlConfig, clock))) {
      AtomicInteger count = new AtomicInteger(0);

      doAnswer(a -> {
        ConsumerRebalanceListener listener = a.getArgument(1);
        listener.onPartitionsAssigned(ImmutableList.of(TP0, TP1));
        return null;
      }).when(kafkaConsumer).subscribe(any(Collection.class), any());
      when(kafkaConsumer.poll(any())).thenAnswer(
          a -> {
            count.incrementAndGet();
            if (count.get() == 1) {
              return RECORDS_FROM_OFFSET2;
            } else {
              consumer.close();
              return EMPTY_RECORDS;
            }
          });
      when(kafkaConsumer.position(TP0)).thenReturn(1L, 4L);
      when(kafkaConsumer.position(TP1)).thenReturn(1L, 4L);
      when(kafkaConsumer.offsetsForTimes(ImmutableMap.of(TP0, 30000L, TP1, 30000L))).thenReturn(
          ImmutableMap.of(TP0, new OffsetAndTimestamp(2L, 30000L),
              TP1, new OffsetAndTimestamp(2L, 30000L))
      );
      when(kafkaConsumer.committed(ImmutableSet.of(TP0, TP1))).thenReturn(
          ImmutableMap.of(TP0, new OffsetAndMetadata(2L),
              TP1, new OffsetAndMetadata(2L))
      );

      consumer.register(queue);
      consumer.run();

      verify(kafkaConsumer, never()).seekToEnd(any());
      verifyRows(
          ImmutableList.of(RECORD0_2, RECORD0_3, RECORD1_2, RECORD1_3));
      verify(consumer, times(1)).initialize();
      verify(consumer, times(1)).onNewAssignment();
      verify(consumer, times(1)).afterCommit();
      verify(consumer, times(1)).onEmptyRecords();
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 4L, TP1, 4L)));
    }
  }

  private void verifyRows(Collection<ConsumerRecord<GenericKey, GenericRow>> records) {
    InOrder inOrder = inOrder(queue);
    for (ConsumerRecord<GenericKey, GenericRow> record : records) {
      inOrder.verify(queue).offer(
          eq(Row.of(SCHEMA, record.key(), record.value(), record.timestamp())));
    }
    inOrder.verify(queue).close();
    inOrder.verifyNoMoreInteractions();
  }

  private static ConsumerRecord<GenericKey, GenericRow> createRecord(final int partition,
      final long offset, final long timestamp, final GenericKey key, final GenericRow row) {
    return new ConsumerRecord<>(TOPIC, partition, offset, timestamp,
        TimestampType.NO_TIMESTAMP_TYPE, -1, -1, key, row, new RecordHeaders(), Optional.empty());
  }
}
