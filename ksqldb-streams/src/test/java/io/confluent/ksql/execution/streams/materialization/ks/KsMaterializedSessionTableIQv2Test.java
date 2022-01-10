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

package io.confluent.ksql.execution.streams.materialization.ks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Range;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.MaterializationTimeOutException;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsMaterializedSessionTableIQv2Test {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .build();

  private static final GenericKey A_KEY = GenericKey.genericKey("x");
  private static final GenericRow A_VALUE = GenericRow.genericRow("c0l");
  private static final int PARTITION = 0;

  private static final Instant LOWER_INSTANT = Instant.ofEpochMilli(System.currentTimeMillis());
  private static final Instant UPPER_INSTANT = LOWER_INSTANT.plusSeconds(10);

  private static final Range<Instant> WINDOW_START_BOUNDS = Range.closed(
      LOWER_INSTANT,
      UPPER_INSTANT
  );

  private static final Range<Instant> WINDOW_END_BOUNDS = Range.closed(
      LOWER_INSTANT,
      UPPER_INSTANT
  );

  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private KsStateStore stateStore;

  @Mock
  private KeyValueIterator<Windowed<GenericKey>, GenericRow> fetchIterator;
  private KsMaterializedSessionTableIQv2 table;
  private final List<KeyValue<Windowed<GenericKey>, GenericRow>> sessions = new ArrayList<>();
  private int sessionIdx;

  @Before
  public void setUp() {
    table = new KsMaterializedSessionTableIQv2(stateStore);

    when(stateStore.getKafkaStreams()).thenReturn(kafkaStreams);
    when(stateStore.schema()).thenReturn(SCHEMA);
    final StateQueryResult result = new StateQueryResult();
    result.addResult(PARTITION, QueryResult.forResult(fetchIterator));
    when(kafkaStreams.query(any())).thenReturn(result);

    sessions.clear();
    sessionIdx = 0;

    when(fetchIterator.hasNext()).thenAnswer(inv -> sessionIdx < sessions.size());
    when(fetchIterator.next()).thenAnswer(inv -> sessions.get(sessionIdx++));


  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(KsStateStore.class, stateStore)
        .testConstructors(KsMaterializedSessionTable.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldThrowIfQueryThrows() {
    // Given:
    when(kafkaStreams.query(any())).thenThrow(new MaterializationTimeOutException("Boom"));

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Boom"));
    assertThat(e, (instanceOf(MaterializationTimeOutException.class)));
  }

  @Test
  public void shouldThrowIfQueryFails() {
    // Given:
    final StateQueryResult partitionResult = new StateQueryResult();
    partitionResult.addResult(PARTITION, QueryResult.forFailure(FailureReason.STORE_EXCEPTION, "Boom"));
    when(kafkaStreams.query(any())).thenReturn(partitionResult);

    // When:
    final Exception e = assertThrows(
      MaterializationException.class,
      () -> table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
      "Boom"));
    assertThat(e, (instanceOf(MaterializationException.class)));
  }

  @Test
  public void shouldReturnValueIfSessionStartsAtLowerBoundIfLowerStartBoundClosed() {
    // Given:
    final Range<Instant> startBounds = Range.closed(
      LOWER_INSTANT,
      UPPER_INSTANT
    );

    final Instant wend = LOWER_INSTANT.plusMillis(1);
    givenSingleSession(LOWER_INSTANT, wend);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, startBounds, Range.all());

    // Then:
    assertThat(result, contains(WindowedRow.of(
      SCHEMA,
      sessionKey(LOWER_INSTANT, wend),
      A_VALUE,
      wend.toEpochMilli()
    )));
  }

  @Test
  public void shouldCloseIterator() {
    // When:
    table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    verify(fetchIterator).close();
  }

  @Test
  public void shouldReturnEmptyIfKeyNotPresent() {
    // When:
    final List<?> result = table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldIgnoreSessionsThatFinishBeforeLowerBound() {
    // Given:
    givenSingleSession(LOWER_INSTANT.minusMillis(1), LOWER_INSTANT.minusMillis(1));

    // When:
    final List<?> result = table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldIgnoreSessionsThatStartAfterUpperBound() {
    // Given:
    givenSingleSession(UPPER_INSTANT.plusMillis(1), UPPER_INSTANT.plusMillis(1));

    // When:
    final List<?> result = table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldIgnoreSessionsThatStartAtLowerBoundIfLowerBoundOpen() {
    // Given:
    final Range<Instant> startBounds = Range.openClosed(
      LOWER_INSTANT,
      UPPER_INSTANT
    );

    givenSingleSession(LOWER_INSTANT, LOWER_INSTANT.plusMillis(1));

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, startBounds, Range.all());

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldReturnValueIfSessionStartsAtUpperBoundIfUpperBoundClosed() {
    // Given:
    final Range<Instant> startBounds = Range.closed(
      LOWER_INSTANT,
      UPPER_INSTANT
    );

    final Instant wend = UPPER_INSTANT.plusMillis(1);
    givenSingleSession(UPPER_INSTANT, wend);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, startBounds, Range.all());

    // Then:
    assertThat(result, contains(WindowedRow.of(
      SCHEMA,
      sessionKey(UPPER_INSTANT, wend),
      A_VALUE,
      wend.toEpochMilli()
    )));
  }

  @Test
  public void shouldIgnoreSessionsThatStartAtUpperBoundIfUpperBoundOpen() {
    // Given:
    final Range<Instant> startBounds = Range.closedOpen(
      LOWER_INSTANT,
      UPPER_INSTANT
    );

    givenSingleSession(UPPER_INSTANT, UPPER_INSTANT.plusMillis(1));

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, startBounds, Range.all());

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldReturnValueIfSessionStartsBetweenBounds() {
    // Given:
    final Instant wend = UPPER_INSTANT.plusMillis(5);
    givenSingleSession(LOWER_INSTANT.plusMillis(1), wend);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, Range.all());

    // Then:
    assertThat(result, contains(WindowedRow.of(
      SCHEMA,
      sessionKey(LOWER_INSTANT.plusMillis(1), wend),
      A_VALUE,
      wend.toEpochMilli()
    )));
  }

  @Test
  public void shouldReturnValueIfSessionEndsAtLowerBoundIfLowerStartBoundClosed() {
    // Given:
    final Range<Instant> endBounds = Range.closed(
      LOWER_INSTANT,
      UPPER_INSTANT
    );

    final Instant wstart = LOWER_INSTANT.minusMillis(1);
    givenSingleSession(wstart, LOWER_INSTANT);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, Range.all(), endBounds);

    // Then:
    assertThat(result, contains(WindowedRow.of(
      SCHEMA,
      sessionKey(wstart, LOWER_INSTANT),
      A_VALUE,
      LOWER_INSTANT.toEpochMilli()
    )));
  }

  @Test
  public void shouldIgnoreSessionsThatEndAtLowerBoundIfLowerBoundOpen() {
    // Given:
    final Range<Instant> endBounds = Range.openClosed(
      LOWER_INSTANT,
      UPPER_INSTANT
    );

    givenSingleSession(LOWER_INSTANT.minusMillis(1), LOWER_INSTANT);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, Range.all(), endBounds);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldReturnValueIfSessionEndsAtUpperBoundIfUpperBoundClosed() {
    // Given:
    final Range<Instant> endBounds = Range.closed(
      LOWER_INSTANT,
      UPPER_INSTANT
    );

    final Instant wstart = UPPER_INSTANT.minusMillis(1);
    givenSingleSession(wstart, UPPER_INSTANT);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, Range.all(), endBounds);

    // Then:
    assertThat(result, contains(WindowedRow.of(
      SCHEMA,
      sessionKey(wstart, UPPER_INSTANT),
      A_VALUE,
      UPPER_INSTANT.toEpochMilli()
    )));
  }

  @Test
  public void shouldIgnoreSessionsThatEndAtUpperBoundIfUpperBoundOpen() {
    // Given:
    final Range<Instant> endBounds = Range.closedOpen(
      LOWER_INSTANT,
      UPPER_INSTANT
    );

    givenSingleSession(UPPER_INSTANT.minusMillis(1), UPPER_INSTANT);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, Range.all(), endBounds);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldReturnValueIfSessionEndsBetweenBounds() {
    // Given:
    final Instant wstart = LOWER_INSTANT.minusMillis(5);
    final Instant wend = UPPER_INSTANT.minusMillis(1);
    givenSingleSession(wstart, wend);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, Range.all(), WINDOW_END_BOUNDS);

    // Then:
    assertThat(result, contains(WindowedRow.of(
      SCHEMA,
      sessionKey(wstart, wend),
      A_VALUE,
      wend.toEpochMilli()
    )));
  }

  @Test
  public void shouldReturnMultipleSessions() {
    // Given:
    givenSingleSession(LOWER_INSTANT.minusMillis(1), LOWER_INSTANT.plusSeconds(1));
    final Instant wend0 = LOWER_INSTANT;
    givenSingleSession(LOWER_INSTANT, wend0);
    final Instant wend1 = UPPER_INSTANT;
    givenSingleSession(UPPER_INSTANT, wend1);
    givenSingleSession(UPPER_INSTANT.plusMillis(1), UPPER_INSTANT.plusSeconds(1));

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS,
      WINDOW_END_BOUNDS);

    // Then:
    assertThat(result, contains(
      WindowedRow.of(
        SCHEMA,
        sessionKey(LOWER_INSTANT, wend0),
        A_VALUE,
        wend0.toEpochMilli()
      ),
      WindowedRow.of(
        SCHEMA,
        sessionKey(UPPER_INSTANT, wend1),
        A_VALUE,
        wend1.toEpochMilli()
      )
    ));
  }

  @Test
  public void shouldReturnAllSessionsForRangeAll() {
    // Given:
    givenSingleSession(Instant.now().minusSeconds(1000), Instant.now().plusSeconds(1000));
    givenSingleSession(Instant.now().minusSeconds(1000), Instant.now().plusSeconds(1000));

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, Range.all(), Range.all());

    // Then:
    assertThat(result, hasSize(2));
  }

  private void givenSingleSession(
    final Instant start,
    final Instant end
  ) {
    sessions.add(new KeyValue<>(sessionKey(start, end), A_VALUE));
  }

  private static Windowed<GenericKey> sessionKey(
    final Instant sessionStart,
    final Instant sessionEnd
  ) {
    return new Windowed<>(
      A_KEY,
      new SessionWindow(sessionStart.toEpochMilli(), sessionEnd.toEpochMilli())
    );
  }
}