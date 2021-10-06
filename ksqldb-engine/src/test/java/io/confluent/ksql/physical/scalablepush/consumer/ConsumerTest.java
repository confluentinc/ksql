package io.confluent.ksql.physical.scalablepush.consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
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
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
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

  ConsumerRecords<GenericKey, GenericRow> RECORDS_JUST0 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(RECORD0_0, RECORD0_1, RECORD0_2))
  );
  ConsumerRecords<GenericKey, GenericRow> RECORDS_JUST1 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP1, ImmutableList.of(RECORD1_0, RECORD1_1, RECORD1_2))
  );

  @Mock
  private Windowed<GenericKey> windowed;

  private static final ConsumerRecord<Windowed<GenericKey>, GenericRow> WRECORD0_0
      = createRecord(0, 0, 0L,
      new Windowed<>(GenericKey.fromList(ImmutableList.of("k00")), new TimeWindow(0, 100)),
      GenericRow.fromList(ImmutableList.of(0)));
  private static final ConsumerRecord<Windowed<GenericKey>, GenericRow> WRECORD0_1
      = createRecord(0, 1, 1L,
      new Windowed<>(GenericKey.fromList(ImmutableList.of("k01")), new TimeWindow(100, 200)),
      GenericRow.fromList(ImmutableList.of(1)));
  private static final ConsumerRecord<Windowed<GenericKey>, GenericRow> WRECORD0_2
      = createRecord(0, 2, 2L,
      new Windowed<>(GenericKey.fromList(ImmutableList.of("k02")), new TimeWindow(200, 300)),
      GenericRow.fromList(ImmutableList.of(2)));
  private static final ConsumerRecord<Windowed<GenericKey>, GenericRow> WRECORD1_0
      = createRecord(1, 0, 3L,
      new Windowed<>(GenericKey.fromList(ImmutableList.of("k10")), new TimeWindow(0, 100)),
      GenericRow.fromList(ImmutableList.of(10)));
  private static final ConsumerRecord<Windowed<GenericKey>, GenericRow> WRECORD1_1
      = createRecord(1, 1, 4L,
      new Windowed<>(GenericKey.fromList(ImmutableList.of("k11")), new TimeWindow(100, 200)),
          GenericRow.fromList(ImmutableList.of(11)));
  private static final ConsumerRecord<Windowed<GenericKey>, GenericRow> WRECORD1_2
      = createRecord(1, 2, 5L,
      new Windowed<>(GenericKey.fromList(ImmutableList.of("k12")), new TimeWindow(200, 300)),
  GenericRow.fromList(ImmutableList.of(12)));

  ConsumerRecords<Windowed<GenericKey>, GenericRow> WRECORDS1 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(WRECORD0_0, WRECORD0_1),
          TP1, ImmutableList.of(WRECORD1_0))
  );
  ConsumerRecords<Windowed<GenericKey>, GenericRow> WRECORDS2 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(WRECORD0_2),
          TP1, ImmutableList.of(WRECORD1_1, WRECORD1_2))
  );

  ConsumerRecords<GenericKey, GenericRow> EMPTY_RECORDS = new ConsumerRecords<>(
      ImmutableMap.of());

  ConsumerRecords<Windowed<GenericKey>, GenericRow> WEMPTY_RECORDS = new ConsumerRecords<>(
      ImmutableMap.of());

  @Mock
  private KafkaConsumer<Object, GenericRow> kafkaConsumer;
  @Mock
  private ProcessingQueue queue;

  @Before
  public void setUp() {
    when(queue.getQueryId()).thenReturn(new QueryId("a"));
    createRecord(0, 0, 0L,
        new Windowed<>(GenericKey.fromList(ImmutableList.of("k00")), new TimeWindow(0, 100)),
        GenericRow.fromList(ImmutableList.of(0)));
  }

  @Test
  public void shouldRunConsumer_success() {
    try (Consumer consumer = spy(new TestConsumer(kafkaConsumer, false))) {
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
      when(kafkaConsumer.position(TP0)).thenReturn(0L, 2L, 3L);
      when(kafkaConsumer.position(TP1)).thenReturn(0L, 1L, 3L);

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
      verify(consumer, times(1)).onNewAssignment();
      verify(consumer, times(2)).afterCommit();
      verify(consumer, times(1)).onEmptyRecords();
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 3L, TP1, 3L)));
    }
  }

  @Test
  public void shouldRunConsumer_successWindowed() {
    try (Consumer consumer = spy(new TestConsumer(kafkaConsumer, true))) {
      AtomicInteger count = new AtomicInteger(0);

      when(kafkaConsumer.poll(any())).thenAnswer(
          a -> {
            count.incrementAndGet();
            if (count.get() == 1) {
              return WRECORDS1;
            } else if (count.get() == 2) {
              return WRECORDS2;
            } else {
              consumer.close();
              return WEMPTY_RECORDS;
            }
          });
      when(kafkaConsumer.position(TP0)).thenReturn(0L, 2L, 3L);
      when(kafkaConsumer.position(TP1)).thenReturn(0L, 1L, 3L);

      // Simulate getting an assignment after a call to initialize
      doAnswer(a -> {
        consumer.newAssignment(ImmutableList.of(TP0, TP1));
        return null;
      }).when(consumer).initialize();

      consumer.register(queue);

      consumer.run();

      verifyRowsW(
          ImmutableList.of(WRECORD0_0, WRECORD0_1, WRECORD1_0, WRECORD0_2, WRECORD1_1, WRECORD1_2));
      verify(consumer, times(1)).initialize();
      verify(consumer, times(1)).onNewAssignment();
      verify(consumer, times(2)).afterCommit();
      verify(consumer, times(1)).onEmptyRecords();
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 3L, TP1, 3L)));
    }
  }

  @Test
  public void shouldRunConsumer_commitError() {
    try (Consumer consumer = spy(new TestConsumer(kafkaConsumer, false))) {
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
      doAnswer(a -> {
        if (count.get() == 2) {
          throw new CommitFailedException();
        }
        return null;
      }).when(kafkaConsumer).commitSync();

      // Simulate getting an assignment after a call to initialize
      doAnswer(a -> {
        consumer.newAssignment(ImmutableList.of(TP0));
        return null;
      }).when(consumer).initialize();

      consumer.register(queue);

      consumer.run();

      verifyRows(
          ImmutableList.of(RECORD0_0, RECORD0_1, RECORD0_2, RECORD1_0, RECORD1_1, RECORD1_2));
      verify(consumer, times(1)).initialize();
      verify(consumer, times(2)).onNewAssignment();
      verify(consumer, times(2)).afterCommit();
      verify(consumer, times(1)).onEmptyRecords();
      assertThat(consumer.getCurrentOffsets(), is(ImmutableMap.of(TP0, 0L, TP1, 3L)));
    }
  }

  @Test
  public void shouldRunConsumer_noAssignmentYet() {
    try (Consumer consumer = spy(new TestConsumer(kafkaConsumer, false))) {
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

      // Simulate getting no assignment yet
      doAnswer(a -> {
        consumer.newAssignment(null);
        return null;
      }).when(consumer).initialize();

      consumer.register(queue);

      consumer.run();

      verifyRows(ImmutableList.of());
      verify(consumer, times(1)).initialize();
      verify(consumer, times(0)).onNewAssignment();
      verify(consumer, times(0)).afterCommit();
      verify(consumer, times(1)).onEmptyRecords();
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

  private void verifyRowsW(Collection<ConsumerRecord<Windowed<GenericKey>, GenericRow>> records) {
    InOrder inOrder = inOrder(queue);
    for (ConsumerRecord<Windowed<GenericKey>, GenericRow> record : records) {
      inOrder.verify(queue).offer(
          eq(WindowedRow.of(SCHEMA, record.key(), record.value(), record.timestamp())));
    }
    inOrder.verify(queue).close();
    inOrder.verifyNoMoreInteractions();
  }

  private static ConsumerRecord<GenericKey, GenericRow> createRecord(final int partition,
      final long offset, final long timestamp, final GenericKey key, final GenericRow row) {
    return new ConsumerRecord<>(TOPIC, partition, offset, timestamp,
        TimestampType.NO_TIMESTAMP_TYPE, -1, -1, key, row, new RecordHeaders(), Optional.empty());
  }

  private static ConsumerRecord<Windowed<GenericKey>, GenericRow> createRecord(final int partition,
      final long offset, final long timestamp, final Windowed<GenericKey> key,
      final GenericRow row) {
    return new ConsumerRecord<>(TOPIC, partition, offset, timestamp,
        TimestampType.NO_TIMESTAMP_TYPE, -1, -1, key, row, new RecordHeaders(), Optional.empty());
  }

  private static class TestConsumer extends Consumer {
    public TestConsumer(final KafkaConsumer<Object, GenericRow> kafkaConsumer, boolean windowed) {
      super(NUM_PARTITIONS, TOPIC, windowed, SCHEMA, kafkaConsumer);
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
  }
}
