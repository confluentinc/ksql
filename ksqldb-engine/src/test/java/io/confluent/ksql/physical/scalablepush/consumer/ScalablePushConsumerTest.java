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
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORD1_0;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORD1_1;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORD1_2;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORDS1;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.RECORDS2;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.SCHEMA;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.TOPIC;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.TP0;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.TP1;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.WEMPTY_RECORDS;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.WRECORD0_0;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.WRECORD0_1;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.WRECORD0_2;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.WRECORD1_0;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.WRECORD1_1;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.WRECORD1_2;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.WRECORDS1;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.WRECORDS2;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.expectPoll;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.expectPollW;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.verifyRows;
import static io.confluent.ksql.physical.scalablepush.consumer.CommonTestUtil.verifyRowsW;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.query.QueryId;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ScalablePushConsumerTest {
  ConsumerRecords<GenericKey, GenericRow> RECORDS_JUST0 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(RECORD0_0, RECORD0_1, RECORD0_2))
  );
  ConsumerRecords<GenericKey, GenericRow> RECORDS_JUST1 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP1, ImmutableList.of(RECORD1_0, RECORD1_1, RECORD1_2))
  );

  @Mock
  private KafkaConsumer<Object, GenericRow> kafkaConsumer;
  @Mock
  private ProcessingQueue queue;
  @Mock
  PartitionInfo partitionInfo1;
  @Mock
  PartitionInfo partitionInfo2;

  @Before
  public void setUp() {
    when(queue.getQueryId()).thenReturn(new QueryId("a"));
    when(kafkaConsumer.partitionsFor(any()))
        .thenReturn(ImmutableList.of(partitionInfo1, partitionInfo2));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRunConsumer_success() {
    try (TestScalablePushConsumer consumer = new TestScalablePushConsumer(kafkaConsumer, false,
        ImmutableList.of(TP0, TP1))) {
      expectPoll(kafkaConsumer, consumer, RECORDS1, RECORDS2, EMPTY_RECORDS);
      when(kafkaConsumer.position(TP0)).thenReturn(0L, 2L, 3L);
      when(kafkaConsumer.position(TP1)).thenReturn(0L, 1L, 3L);

      consumer.register(queue);

      consumer.run();

      verifyRows(
          queue,
          ImmutableList.of(RECORD0_0, RECORD0_1, RECORD1_0, RECORD0_2, RECORD1_1, RECORD1_2));
      assertThat(consumer.getNewAssignment(), is (1));
      assertThat(consumer.getAfterCommit(), is (2));
      assertThat(consumer.getEmptyRecords(), is (1));
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 3L, TP1, 3L)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRunConsumer_successWindowed() {
    try (TestScalablePushConsumer consumer = new TestScalablePushConsumer(kafkaConsumer, true,
        ImmutableList.of(TP0, TP1))) {
      expectPollW(kafkaConsumer, consumer, WRECORDS1, WRECORDS2, WEMPTY_RECORDS);
      when(kafkaConsumer.position(TP0)).thenReturn(0L, 2L, 3L);
      when(kafkaConsumer.position(TP1)).thenReturn(0L, 1L, 3L);

      consumer.register(queue);

      consumer.run();

      verifyRowsW(
          queue,
          ImmutableList.of(WRECORD0_0, WRECORD0_1, WRECORD1_0, WRECORD0_2, WRECORD1_1, WRECORD1_2));
      assertThat(consumer.getNewAssignment(), is (1));
      assertThat(consumer.getAfterCommit(), is (2));
      assertThat(consumer.getEmptyRecords(), is (1));
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 3L, TP1, 3L)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRunConsumer_commitError() {
    try (TestScalablePushConsumer consumer = new TestScalablePushConsumer(kafkaConsumer, false,
        ImmutableList.of(TP0, TP1))) {
      expectPoll(kafkaConsumer, consumer, RECORDS_JUST0, RECORDS_JUST1, EMPTY_RECORDS);
      when(kafkaConsumer.position(TP0)).thenReturn(0L, 3L);
      when(kafkaConsumer.position(TP1)).thenReturn(0L, 3L);
      doAnswer(a -> {
        throw new CommitFailedException();
      }).when(kafkaConsumer).commitSync();

      consumer.register(queue);

      consumer.run();

      verifyRows(
          queue,
          ImmutableList.of(RECORD0_0, RECORD0_1, RECORD0_2, RECORD1_0, RECORD1_1, RECORD1_2));
      assertThat(consumer.getNewAssignment(), is (1));
      assertThat(consumer.getAfterCommit(), is (2));
      assertThat(consumer.getEmptyRecords(), is (1));
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 3L, TP1, 3L)));
    }
  }

  @Test
  public void shouldRunConsumer_reassign() {
    try (TestScalablePushConsumer consumer = new TestScalablePushConsumer(kafkaConsumer, false,
        ImmutableList.of(TP0))) {
      AtomicInteger count = new AtomicInteger(0);
      when(kafkaConsumer.poll(any())).thenAnswer(
          a -> {
            count.incrementAndGet();
            if (count.get() == 1) {
              return RECORDS_JUST0;
            } else if (count.get() == 2) {
              consumer.newAssignment(ImmutableList.of(TP1));
              return RECORDS_JUST1;
            } else {
              consumer.close();
              return EMPTY_RECORDS;
            }
          });
      when(kafkaConsumer.position(TP0)).thenReturn(0L, 3L);
      when(kafkaConsumer.position(TP1)).thenReturn(0L, 3L);

      consumer.register(queue);

      consumer.run();

      verifyRows(
          queue,
          ImmutableList.of(RECORD0_0, RECORD0_1, RECORD0_2, RECORD1_0, RECORD1_1, RECORD1_2));
      assertThat(consumer.getNewAssignment(), is (2));
      assertThat(consumer.getAfterCommit(), is (2));
      assertThat(consumer.getEmptyRecords(), is (1));
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 0L, TP1, 3L)));
    }
  }

  @Test
  public void shouldRunConsumer_noAssignmentYet() {
    try (TestScalablePushConsumer consumer = new TestScalablePushConsumer(kafkaConsumer, false,
        null)) {
      AtomicInteger count = new AtomicInteger(0);

      when(kafkaConsumer.poll(any())).thenAnswer(
          a -> {
            count.incrementAndGet();
            if (count.get() == 2) {
              consumer.newAssignment(ImmutableList.of(TP0, TP1));
              consumer.close();
            }
            return EMPTY_RECORDS;
          });

      consumer.register(queue);

      consumer.run();

      verifyRows(queue, ImmutableList.of());
      assertThat(consumer.getNewAssignment(), is (0));
      assertThat(consumer.getAfterCommit(), is (0));
      assertThat(consumer.getEmptyRecords(), is (1));
    }
  }

  @Test
  public void shouldRunConsumer_closeAsync() {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    AtomicReference<TestScalablePushConsumer> ref = new AtomicReference<>();
    try {
      when(kafkaConsumer.poll(any())).thenAnswer(a -> EMPTY_RECORDS);
      executorService.submit(() -> {
        try (TestScalablePushConsumer consumer = new TestScalablePushConsumer(kafkaConsumer, false,
            null)) {
          ref.set(consumer);

          consumer.register(queue);

          consumer.run();
          ref.set(null);
        }
      });

      assertThatEventually(ref::get, notNullValue());
      ref.get().closeAsync();
      assertThatEventually(ref::get, nullValue());
    } finally {
      executorService.shutdownNow();
    }
  }

  private static class TestScalablePushConsumer extends ScalablePushConsumer {

    private final List<TopicPartition> initialAssignment;
    private int emptyRecords = 0;
    private int afterCommit = 0;
    private int newAssignment = 0;

    public TestScalablePushConsumer(final KafkaConsumer<Object, GenericRow> kafkaConsumer,
        boolean windowed,
        final List<TopicPartition> initialAssignment) {
      super(TOPIC, windowed, SCHEMA, kafkaConsumer);
      this.initialAssignment = initialAssignment;
    }

    @Override
    protected boolean onEmptyRecords() {
      emptyRecords++;
      return false;
    }

    @Override
    protected boolean afterCommit() {
      afterCommit++;
      return false;
    }

    @Override
    protected void onNewAssignment() {
      newAssignment++;
    }

    @Override
    protected void subscribeOrAssign() {
      // Simulate getting an assignment after a call to subscribeOrAssign
      newAssignment(initialAssignment);
    }

    public int getEmptyRecords() {
      return emptyRecords;
    }

    public int getAfterCommit() {
      return afterCommit;
    }

    public int getNewAssignment() {
      return newAssignment;
    }
  }
}
