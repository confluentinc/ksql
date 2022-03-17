package io.confluent.ksql.execution.scalablepush.consumer;

import static io.confluent.ksql.execution.scalablepush.consumer.CatchupConsumer.WAIT_FOR_ASSIGNMENT_MS;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.EMPTY_RECORDS;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.QR0_1;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.QR0_2;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.QR1_2;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.QR1_3;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.RECORD0_1;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.RECORD0_2;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.RECORD1_2;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.RECORD1_3;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.SCHEMA;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.TOPIC;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.TP0;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.TP1;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.expectPoll;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.offsetsRow;
import static io.confluent.ksql.execution.scalablepush.consumer.CommonTestUtil.verifyQueryRows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.scalablepush.ProcessingQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PushOffsetRange;
import io.confluent.ksql.util.PushOffsetVector;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CatchupConsumerTest {

  private static final long CURRENT_TIME_MS = 60000;

  @Mock
  private KafkaConsumer<Object, GenericRow> kafkaConsumer;
  @Mock
  private ProcessingQueue queue;
  @Mock
  private Clock clock;
  @Mock
  private LatestConsumer latestConsumer;
  @Mock
  private Consumer<Long> sleepFn;
  @Mock
  private BiConsumer<Object, Long> waitFn;

  private boolean caughtUp = false;
  private CatchupCoordinatorImpl catchupCoordinator = new CatchupCoordinatorImpl();

  @Before
  public void setUp() {
    when(queue.getQueryId()).thenReturn(new QueryId("a"));
    when(clock.millis()).thenReturn(CURRENT_TIME_MS);
    when(kafkaConsumer.partitionsFor(TOPIC)).thenReturn(
        ImmutableList.of(mock(PartitionInfo.class), mock(PartitionInfo.class)));
    when(latestConsumer.getAssignment()).thenReturn(ImmutableSet.of(TP0, TP1));
  }

  @Test
  public void shouldRunConsumer_success_switchOver() {
    // Given:
    PushOffsetRange offsetRange = new PushOffsetRange(Optional.empty(),
        new PushOffsetVector(ImmutableList.of(1L, 2L)));
    try (CatchupConsumer consumer = new CatchupConsumer(
        TOPIC, false, SCHEMA, kafkaConsumer, () -> latestConsumer, catchupCoordinator,
        offsetRange, clock, 0, pq -> caughtUp = true)) {

      runSuccessfulTest(consumer);
    }
  }

  @Test
  public void shouldRunConsumer_success_waitForLatestAssignment() {
    // Given:
    PushOffsetRange offsetRange = new PushOffsetRange(Optional.empty(),
        new PushOffsetVector(ImmutableList.of(1L, 2L)));
    when(latestConsumer.getAssignment()).thenReturn(null);
    AtomicReference<CatchupConsumer> cRef = new AtomicReference<>();
    // Rather than wait, simulate the latest getting an assignment
    final BiConsumer<Object, Long> waitFn = (o, wait) ->
        cRef.get().newAssignment(ImmutableSet.of(TP0, TP1));
    try (CatchupConsumer consumer = new CatchupConsumer(
        TOPIC, false, SCHEMA, kafkaConsumer, () -> latestConsumer, catchupCoordinator,
        offsetRange, clock, sleepFn, waitFn, 0, pq -> caughtUp = true)) {
      cRef.set(consumer);

      runSuccessfulTest(consumer);
    }
  }

  @Test
  public void shouldRunConsumer_timeoutWaitingForAssignment() {
    // Given:
    PushOffsetRange offsetRange = new PushOffsetRange(Optional.empty(),
        new PushOffsetVector(ImmutableList.of(1L, 2L)));
    when(latestConsumer.getAssignment()).thenReturn(null);
    when(clock.millis()).thenReturn(
        CURRENT_TIME_MS, CURRENT_TIME_MS + WAIT_FOR_ASSIGNMENT_MS -
            1, CURRENT_TIME_MS + WAIT_FOR_ASSIGNMENT_MS + 1);
    try (CatchupConsumer consumer = new CatchupConsumer(
        TOPIC, false, SCHEMA, kafkaConsumer, () -> latestConsumer, catchupCoordinator,
        offsetRange, clock, sleepFn, waitFn, 0, pq -> caughtUp = true)) {

      // When:
      consumer.register(queue);

      final Exception e = assertThrows(
          KsqlException.class,
          consumer::run
      );

      // Then:
      assertThat(e.getMessage(), containsString("Timed out waiting for assignment from Latest"));
    }
  }

  @Test
  public void shouldRunConsumer_queueIsAtLimit() {
    // Given:
    PushOffsetRange offsetRange = new PushOffsetRange(Optional.empty(),
        new PushOffsetVector(ImmutableList.of(1L, 2L)));
    when(queue.isAtLimit()).thenReturn(false, true, true, false);
    try (CatchupConsumer consumer = new CatchupConsumer(
        TOPIC, false, SCHEMA, kafkaConsumer, () -> latestConsumer, catchupCoordinator,
        offsetRange, clock, sleepFn, waitFn, 0, pq -> caughtUp = true)) {

      // When:
      consumer.register(queue);

      runSuccessfulTest(consumer);
      verify(sleepFn, times(2)).accept(any());
    }
  }

  @SuppressWarnings("unchecked")
  private void runSuccessfulTest(final CatchupConsumer consumer) {
    // When:
    expectPoll(kafkaConsumer, () -> catchupCoordinator.simulateWaitingInTest(),
        new ConsumerRecords<>(
            ImmutableMap.of(
                TP0, ImmutableList.of(RECORD0_1),
                TP1, ImmutableList.of(RECORD1_2))
        ),
        new ConsumerRecords<>(
            ImmutableMap.of(
                TP0, ImmutableList.of(RECORD0_2),
                TP1, ImmutableList.of(RECORD1_3))
        ), EMPTY_RECORDS);
    when(kafkaConsumer.position(TP0)).thenReturn(1L, 2L, 3L);
    when(kafkaConsumer.position(TP1)).thenReturn(2L, 3L, 4L);
    when(latestConsumer.getCurrentOffsets()).thenReturn(ImmutableMap.of(TP0, 3L, TP1, 4L));

    consumer.register(queue);
    consumer.run();

    // Then:
    verifyQueryRows(
        queue,
        ImmutableList.of(
            QR0_1, QR1_2,
            offsetsRow(CURRENT_TIME_MS, ImmutableList.of(1L, 2L), ImmutableList.of(2L, 3L)),
            QR0_2, QR1_3,
            offsetsRow(CURRENT_TIME_MS, ImmutableList.of(2L, 3L), ImmutableList.of(3L, 4L)),
            offsetsRow(CURRENT_TIME_MS, ImmutableList.of(3L, 4L), ImmutableList.of(3L, 4L))));

    verify(latestConsumer).register(queue);
    assertThat(caughtUp, is(true));
  }

}
