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

import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.EMPTY_RECORDS;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORD0_0;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORD0_1;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORD0_2;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORD0_3;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORD1_0;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORD1_1;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORD1_2;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORD1_3;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORDS1;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORDS2;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORDS_FROM_OFFSET2;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.SCHEMA;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.TOPIC;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.TP0;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.TP1;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.verifyRows;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.expectPoll;
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
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Clock;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
      when(kafkaConsumer.position(TP0)).thenReturn(0L, 1L, 2L);
      when(kafkaConsumer.position(TP1)).thenReturn(0L, 1L, 2L);

      consumer.register(queue);
      consumer.run();

      verifyRows(
          queue,
          ImmutableList.of(RECORD0_0, RECORD0_1, RECORD1_0, RECORD0_2, RECORD1_1, RECORD1_2));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRunConsumer_seekToEnd() {
    try (LatestConsumer consumer = new LatestConsumer(
        TOPIC, false, SCHEMA, kafkaConsumer, new NoopCatchupCoordinator(),
        catchupAssignmentUpdater, ksqlConfig, clock)) {
      expectPoll(kafkaConsumer, consumer, RECORDS_FROM_OFFSET2, EMPTY_RECORDS);
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
          queue,
          ImmutableList.of(RECORD0_2, RECORD0_3, RECORD1_2, RECORD1_3));
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 4L, TP1, 4L)));
    }
  }
}
