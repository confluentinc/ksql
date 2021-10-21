package io.confluent.ksql.physical.scalablepush.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.mockito.InOrder;

public class CommonTestUtil {

  static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
      .build();
  static final String TOPIC = "topic";
  static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
  static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);

  static final ConsumerRecord<GenericKey, GenericRow> RECORD0_0
      = createRecord(0, 0, 0L,
      GenericKey.fromList(ImmutableList.of("k00")), GenericRow.fromList(ImmutableList.of(0)));
  static final ConsumerRecord<GenericKey, GenericRow> RECORD0_1
      = createRecord(0, 1, 1L,
      GenericKey.fromList(ImmutableList.of("k01")), GenericRow.fromList(ImmutableList.of(1)));
  static final ConsumerRecord<GenericKey, GenericRow> RECORD0_2
      = createRecord(0, 2, 2L,
      GenericKey.fromList(ImmutableList.of("k02")), GenericRow.fromList(ImmutableList.of(2)));
  static final ConsumerRecord<GenericKey, GenericRow> RECORD0_3
      = createRecord(0, 3, 3L,
      GenericKey.fromList(ImmutableList.of("k03")), GenericRow.fromList(ImmutableList.of(3)));
  static final ConsumerRecord<GenericKey, GenericRow> RECORD1_0
      = createRecord(1, 0, 4L,
      GenericKey.fromList(ImmutableList.of("k10")), GenericRow.fromList(ImmutableList.of(10)));
  static final ConsumerRecord<GenericKey, GenericRow> RECORD1_1
      = createRecord(1, 1, 5L,
      GenericKey.fromList(ImmutableList.of("k11")), GenericRow.fromList(ImmutableList.of(11)));
  static final ConsumerRecord<GenericKey, GenericRow> RECORD1_2
      = createRecord(1, 2, 6L,
      GenericKey.fromList(ImmutableList.of("k12")), GenericRow.fromList(ImmutableList.of(12)));
  static final ConsumerRecord<GenericKey, GenericRow> RECORD1_3
      = createRecord(1, 4, 7L,
      GenericKey.fromList(ImmutableList.of("k13")), GenericRow.fromList(ImmutableList.of(13)));

  static final ConsumerRecord<Windowed<GenericKey>, GenericRow> WRECORD0_0
      = createRecord(0, 0, 0L,
      new Windowed<>(GenericKey.fromList(ImmutableList.of("k00")), new TimeWindow(0, 100)),
      GenericRow.fromList(ImmutableList.of(0)));
  static final ConsumerRecord<Windowed<GenericKey>, GenericRow> WRECORD0_1
      = createRecord(0, 1, 1L,
      new Windowed<>(GenericKey.fromList(ImmutableList.of("k01")), new TimeWindow(100, 200)),
      GenericRow.fromList(ImmutableList.of(1)));
  static final ConsumerRecord<Windowed<GenericKey>, GenericRow> WRECORD0_2
      = createRecord(0, 2, 2L,
      new Windowed<>(GenericKey.fromList(ImmutableList.of("k02")), new TimeWindow(200, 300)),
      GenericRow.fromList(ImmutableList.of(2)));
  static final ConsumerRecord<Windowed<GenericKey>, GenericRow> WRECORD1_0
      = createRecord(1, 0, 3L,
      new Windowed<>(GenericKey.fromList(ImmutableList.of("k10")), new TimeWindow(0, 100)),
      GenericRow.fromList(ImmutableList.of(10)));
  static final ConsumerRecord<Windowed<GenericKey>, GenericRow> WRECORD1_1
      = createRecord(1, 1, 4L,
      new Windowed<>(GenericKey.fromList(ImmutableList.of("k11")), new TimeWindow(100, 200)),
      GenericRow.fromList(ImmutableList.of(11)));
  static final ConsumerRecord<Windowed<GenericKey>, GenericRow> WRECORD1_2
      = createRecord(1, 2, 5L,
      new Windowed<>(GenericKey.fromList(ImmutableList.of("k12")), new TimeWindow(200, 300)),
      GenericRow.fromList(ImmutableList.of(12)));

  static final ConsumerRecords<Windowed<GenericKey>, GenericRow> WRECORDS1 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(WRECORD0_0, WRECORD0_1),
          TP1, ImmutableList.of(WRECORD1_0))
  );
  static final ConsumerRecords<Windowed<GenericKey>, GenericRow> WRECORDS2 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(WRECORD0_2),
          TP1, ImmutableList.of(WRECORD1_1, WRECORD1_2))
  );

  static final ConsumerRecords<GenericKey, GenericRow> RECORDS1 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(RECORD0_0, RECORD0_1),
          TP1, ImmutableList.of(RECORD1_0))
  );
  static final  ConsumerRecords<GenericKey, GenericRow> RECORDS2 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(RECORD0_2),
          TP1, ImmutableList.of(RECORD1_1, RECORD1_2))
  );
  static final ConsumerRecords<GenericKey, GenericRow> RECORDS_FROM_OFFSET2 = new ConsumerRecords<>(
      ImmutableMap.of(
          TP0, ImmutableList.of(RECORD0_2, RECORD0_3),
          TP1, ImmutableList.of(RECORD1_2, RECORD1_3))
  );
  static final ConsumerRecords<GenericKey, GenericRow> EMPTY_RECORDS = new ConsumerRecords<>(
      ImmutableMap.of());

  static final ConsumerRecords<Windowed<GenericKey>, GenericRow> WEMPTY_RECORDS
      = new ConsumerRecords<>(ImmutableMap.of());

  static void verifyRows(
      final ProcessingQueue queue,
      final Collection<ConsumerRecord<GenericKey, GenericRow>> records) {
    InOrder inOrder = inOrder(queue);
    for (ConsumerRecord<GenericKey, GenericRow> record : records) {
      inOrder.verify(queue).offer(
          eq(Row.of(SCHEMA, record.key(), record.value(), record.timestamp())));
    }
    inOrder.verify(queue).close();
    inOrder.verifyNoMoreInteractions();
  }

  static void verifyRowsW(
      final ProcessingQueue queue,
      final Collection<ConsumerRecord<Windowed<GenericKey>, GenericRow>> records) {
    InOrder inOrder = inOrder(queue);
    for (ConsumerRecord<Windowed<GenericKey>, GenericRow> record : records) {
      inOrder.verify(queue).offer(
          eq(WindowedRow.of(SCHEMA, record.key(), record.value(), record.timestamp())));
    }
    inOrder.verify(queue).close();
    inOrder.verifyNoMoreInteractions();
  }

  @SuppressWarnings("unchecked")
  static void expectPoll(
      final KafkaConsumer<Object, GenericRow> kafkaConsumer,
      final ScalablePushConsumer consumer,
      final ConsumerRecords<GenericKey, GenericRow>...records) {
    AtomicInteger count = new AtomicInteger(0);
    when(kafkaConsumer.poll(any())).thenAnswer(
        a -> {
          if (count.get() == records.length - 1) {
            consumer.close();
          }
          return records[count.getAndIncrement()];
        });
  }

  @SuppressWarnings("unchecked")
  static void expectPollW(
      final KafkaConsumer<Object, GenericRow> kafkaConsumer,
      final ScalablePushConsumer consumer,
      final ConsumerRecords<Windowed<GenericKey>, GenericRow>...records) {
    AtomicInteger count = new AtomicInteger(0);
    when(kafkaConsumer.poll(any())).thenAnswer(
        a -> {
          if (count.get() == records.length - 1) {
            consumer.close();
          }
          return records[count.getAndIncrement()];
        });
  }

  static ConsumerRecord<GenericKey, GenericRow> createRecord(final int partition,
      final long offset, final long timestamp, final GenericKey key, final GenericRow row) {
    return new ConsumerRecord<>(TOPIC, partition, offset, timestamp,
        TimestampType.NO_TIMESTAMP_TYPE, -1, -1, key, row, new RecordHeaders(), Optional.empty());
  }

  static ConsumerRecord<Windowed<GenericKey>, GenericRow> createRecord(final int partition,
      final long offset, final long timestamp, final Windowed<GenericKey> key,
      final GenericRow row) {
    return new ConsumerRecord<>(TOPIC, partition, offset, timestamp,
        TimestampType.NO_TIMESTAMP_TYPE, -1, -1, key, row, new RecordHeaders(), Optional.empty());
  }
}
