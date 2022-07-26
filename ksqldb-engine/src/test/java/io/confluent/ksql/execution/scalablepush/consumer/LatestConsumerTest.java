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

package io.confluent.ksql.execution.scalablepush.consumer;

import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.EMPTY_RECORDS;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.QR0_0;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.QR0_1;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.QR0_2;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.QR0_3;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.QR1_0;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.QR1_1;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.QR1_2;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.QR1_3;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.RECORD0_2;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.RECORD0_3;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.RECORD1_2;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.RECORD1_3;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.RECORDS1;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.RECORDS2;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.RECORDS_FROM_OFFSET2;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.SCHEMA;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.TOPIC;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.TP0;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.TP1;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.offsetsRow;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.verifyQueryRows;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.verifyRows;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.expectPoll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.scalablepush.ProcessingQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LatestConsumerTest {

  private static final long CURRENT_TIME_MS = 60000;

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

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    when(queue.getQueryId()).thenReturn(new QueryId("a"));
    when(clock.millis()).thenReturn(CURRENT_TIME_MS);
    when(ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_PUSH_V2_LATEST_RESET_AGE_MS)).thenReturn(30000L);
    when(kafkaConsumer.partitionsFor(TOPIC)).thenReturn(
        ImmutableList.of(mock(PartitionInfo.class), mock(PartitionInfo.class)));
    doAnswer(a -> {
      ConsumerRebalanceListener listener = a.getArgument(1);
      listener.onPartitionsAssigned(ImmutableList.of(TP0, TP1));
      return null;
    }).when(kafkaConsumer).subscribe(any(Collection.class), any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRunConsumer_success() {
    try (LatestConsumer consumer = new LatestConsumer(
        TOPIC, false, SCHEMA, kafkaConsumer, new NoopCatchupCoordinator(),
        catchupAssignmentUpdater, ksqlConfig, clock)) {
      expectPoll(kafkaConsumer, consumer, RECORDS1, RECORDS2, EMPTY_RECORDS);
      when(kafkaConsumer.position(TP0)).thenReturn(0L, 2L, 3L);
      when(kafkaConsumer.position(TP1)).thenReturn(0L, 1L, 3L);
      when(kafkaConsumer.offsetsForTimes(ImmutableMap.of(TP0, 30000L, TP1, 30000L))).thenReturn(
          ImmutableMap.of(TP0, new OffsetAndTimestamp(0L, 30000L),
              TP1, new OffsetAndTimestamp(0L, 30000L))
      );
      when(kafkaConsumer.committed(ImmutableSet.of(TP0, TP1))).thenReturn(
          ImmutableMap.of(TP0, new OffsetAndMetadata(0L),
              TP1, new OffsetAndMetadata(0L))
      );

      consumer.register(queue);
      consumer.run();

      verifyQueryRows(
          queue,
          ImmutableList.of(
              QR0_0, QR0_1, QR1_0,
              offsetsRow(CURRENT_TIME_MS, ImmutableList.of(0L, 0L), ImmutableList.of(2L, 1L)),
              QR0_2, QR1_1, QR1_2,
              offsetsRow(CURRENT_TIME_MS, ImmutableList.of(2L, 1L), ImmutableList.of(3L, 3L)),
              offsetsRow(CURRENT_TIME_MS, ImmutableList.of(3L, 3L), ImmutableList.of(3L, 3L))));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRunConsumer_seekToEnd() {
    try (LatestConsumer consumer = new LatestConsumer(
        TOPIC, false, SCHEMA, kafkaConsumer, new NoopCatchupCoordinator(),
        catchupAssignmentUpdater, ksqlConfig, clock)) {
      expectPoll(kafkaConsumer, consumer, RECORDS_FROM_OFFSET2, EMPTY_RECORDS);
      when(kafkaConsumer.position(TP0)).thenReturn(1L, 3L, 5L);
      when(kafkaConsumer.position(TP1)).thenReturn(1L, 3L, 5L);
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
      verifyQueryRows(
          queue,
          ImmutableList.of(
              QR0_2, QR0_3, QR1_2, QR1_3,
              offsetsRow(CURRENT_TIME_MS, ImmutableList.of(3L, 3L), ImmutableList.of(5L, 5L)),
              offsetsRow(CURRENT_TIME_MS, ImmutableList.of(5L, 5L), ImmutableList.of(5L, 5L))));
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 5L, TP1, 5L)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRunConsumer_seekToEnd_noTimestamps() {
    try (LatestConsumer consumer = new LatestConsumer(
        TOPIC, false, SCHEMA, kafkaConsumer, new NoopCatchupCoordinator(),
        catchupAssignmentUpdater, ksqlConfig, clock)) {
      expectPoll(kafkaConsumer, consumer, RECORDS_FROM_OFFSET2, EMPTY_RECORDS);
      when(kafkaConsumer.position(TP0)).thenReturn(1L, 4L);
      when(kafkaConsumer.position(TP1)).thenReturn(1L, 4L);
      Map<TopicPartition, OffsetAndTimestamp> timestamps = new HashMap<>();
      timestamps.put(TP0, null);
      timestamps.put(TP1, null);
      when(kafkaConsumer.offsetsForTimes(ImmutableMap.of(TP0, 30000L, TP1, 30000L))).thenReturn(
          timestamps
      );
      when(kafkaConsumer.committed(ImmutableSet.of(TP0, TP1))).thenReturn(
          ImmutableMap.of(TP0, new OffsetAndMetadata(1L),
              TP1, new OffsetAndMetadata(1L))
      );

      consumer.register(queue);
      consumer.run();

      verify(kafkaConsumer).seekToEnd(ImmutableSet.of(TP0, TP1));
      verifyRows(
          queue,
          ImmutableList.of(RECORD0_2, RECORD0_3, RECORD1_2, RECORD1_3));
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 4L, TP1, 4L)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRunConsumer_dontSeekToEnd() {
    try (LatestConsumer consumer = new LatestConsumer(
        TOPIC, false, SCHEMA, kafkaConsumer, new NoopCatchupCoordinator(),
        catchupAssignmentUpdater, ksqlConfig, clock)) {
      expectPoll(kafkaConsumer, consumer, RECORDS_FROM_OFFSET2, EMPTY_RECORDS);
      when(kafkaConsumer.position(TP0)).thenReturn(2L, 4L);
      when(kafkaConsumer.position(TP1)).thenReturn(2L, 4L);
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
          queue,
          ImmutableList.of(RECORD0_2, RECORD0_3, RECORD1_2, RECORD1_3));
      verifyQueryRows(
          queue,
          ImmutableList.of(
              QR0_2, QR0_3, QR1_2, QR1_3,
              offsetsRow(CURRENT_TIME_MS, ImmutableList.of(2L, 2L), ImmutableList.of(4L, 4L)),
              offsetsRow(CURRENT_TIME_MS, ImmutableList.of(4L, 4L), ImmutableList.of(4L, 4L))));
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 4L, TP1, 4L)));
    }
  }
}
