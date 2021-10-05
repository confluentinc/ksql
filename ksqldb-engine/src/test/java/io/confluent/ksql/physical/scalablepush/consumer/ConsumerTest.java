package io.confluent.ksql.physical.scalablepush.consumer;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
public class ConsumerTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
      .build();

  private static final String TOPIC = "topic";
  private static final int NUM_PARTITIONS = 2;
  private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);

  private static final ConsumerRecord<GenericKey, GenericRow> RECORD0_0
      = createRecord(0, 0, 0L,
      GenericKey.fromList(ImmutableList.of("k00")), GenericRow.fromList(ImmutableList.of(0)));
  private static final ConsumerRecord<GenericKey, GenericRow> RECORD0_1
      = createRecord(0, 1, 1L,
      GenericKey.fromList(ImmutableList.of("k01")), GenericRow.fromList(ImmutableList.of(1)));
  private static final ConsumerRecord<GenericKey, GenericRow> RECORD0_2
      = createRecord(0, 2, 2L,
      GenericKey.fromList(ImmutableList.of("k02")), GenericRow.fromList(ImmutableList.of(2)));
  private static final ConsumerRecord<GenericKey, GenericRow> RECORD1_0
      = createRecord(1, 0, 3L,
      GenericKey.fromList(ImmutableList.of("k10")), GenericRow.fromList(ImmutableList.of(10)));
  private static final ConsumerRecord<GenericKey, GenericRow> RECORD1_1
      = createRecord(1, 1, 4L,
      GenericKey.fromList(ImmutableList.of("k11")), GenericRow.fromList(ImmutableList.of(11)));
  private static final ConsumerRecord<GenericKey, GenericRow> RECORD1_2
      = createRecord(1, 2, 5L,
      GenericKey.fromList(ImmutableList.of("k12")), GenericRow.fromList(ImmutableList.of(12)));
  ConsumerRecords<GenericKey, GenericRow> RECORDS1 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(RECORD0_0, RECORD0_1),
          TP1, ImmutableList.of(RECORD1_0))
  );
  ConsumerRecords<GenericKey, GenericRow> RECORDS2 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(RECORD0_2),
          TP1, ImmutableList.of(RECORD1_1, RECORD1_2))
  );

  ConsumerRecords<GenericKey, GenericRow> EMPTY_RECORDS = new ConsumerRecords<>(
      ImmutableMap.of());

  @Mock
  private KafkaConsumer<GenericKey, GenericRow> kafkaConsumer;
  @Mock
  private ProcessingQueue queue;

  @Before
  public void setUp() {
    when(queue.getQueryId()).thenReturn(new QueryId("a"));
  }

  @Test
  public void shouldRunConsumer_success() {
    try (Consumer consumer = spy(new TestConsumer(kafkaConsumer))) {
      AtomicInteger count = new AtomicInteger(0);

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

      // Simulate getting an assignment after a call to initialize
      doAnswer(a -> {
        consumer.newAssignment(ImmutableList.of(TP0, TP1));
        return null;
      }).when(consumer).initialize();

      consumer.register(queue);

      consumer.run();

      verifyRows(
          ImmutableList.of(RECORD0_0, RECORD0_1, RECORD1_0, RECORD0_2, RECORD1_1, RECORD1_2));
      verify(consumer, times(1)).initialize();
      verify(consumer, times(1)).afterFirstPoll();
      verify(consumer, times(1)).onNewAssignment();
      verify(consumer, times(2)).afterCommit();
      verify(consumer, times(1)).onEmptyRecords();
    }
  }

  private void verifyRows(Collection<ConsumerRecord<GenericKey, GenericRow>> records) {
    InOrder inOrder = inOrder(queue);
    for (ConsumerRecord<GenericKey, GenericRow> record : records) {
      inOrder.verify(queue).offer(
          eq(Row.of(SCHEMA, record.key(), record.value(), record.timestamp())));
    }
  }

  private static ConsumerRecord<GenericKey, GenericRow> createRecord(final int partition,
      final long offset, final long timestamp, final GenericKey key, final GenericRow row) {
    return new ConsumerRecord<>(TOPIC, partition, offset, timestamp,
        TimestampType.NO_TIMESTAMP_TYPE, -1, -1, key, row, new RecordHeaders(), Optional.empty());
  }

  private static class TestConsumer extends Consumer {
    public TestConsumer(final KafkaConsumer<GenericKey, GenericRow> kafkaConsumer) {
      super(NUM_PARTITIONS, TOPIC, false, SCHEMA, kafkaConsumer);
    }

    @Override
    protected void onEmptyRecords() {
    }

    @Override
    protected void afterCommit() {
    }

    @Override
    protected void onNewAssignment() {
    }

    @Override
    protected void afterFirstPoll() {
    }
  }
}
