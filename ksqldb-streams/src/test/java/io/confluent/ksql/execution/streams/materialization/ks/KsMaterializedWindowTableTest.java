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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Range;
import com.google.common.collect.Streams;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.MaterializationTimeOutException;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.execution.streams.materialization.ks.WindowStoreCacheBypass.WindowStoreCacheBypassFetcher;
import io.confluent.ksql.execution.streams.materialization.ks.WindowStoreCacheBypass.WindowStoreCacheBypassFetcherAll;
import io.confluent.ksql.execution.streams.materialization.ks.WindowStoreCacheBypass.WindowStoreCacheBypassFetcherRange;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsMaterializedWindowTableTest {

  private static final Duration WINDOW_SIZE = Duration.ofMinutes(1);
  private static final int PARTITION = 0;

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .build();

  private static final GenericKey A_KEY = GenericKey.genericKey(0);
  private static final GenericKey A_KEY2 = GenericKey.genericKey(1);
  private static final GenericKey A_KEY3 = GenericKey.genericKey(1);

  protected static final Instant NOW = Instant.ofEpochMilli(System.currentTimeMillis());

  private static final Range<Instant> WINDOW_START_BOUNDS = Range.closed(
      NOW,
      NOW.plusSeconds(10)
  );

  private static final Range<Instant> WINDOW_END_BOUNDS = Range.closed(
      NOW.plusSeconds(5).plus(WINDOW_SIZE),
      NOW.plusSeconds(15).plus(WINDOW_SIZE)
  );

  private static final ValueAndTimestamp<GenericRow> VALUE_1 = ValueAndTimestamp
      .make(GenericRow.genericRow("col0"), 12345L);
  private static final ValueAndTimestamp<GenericRow> VALUE_2 = ValueAndTimestamp
      .make(GenericRow.genericRow("col1"), 45678L);
  private static final ValueAndTimestamp<GenericRow> VALUE_3 = ValueAndTimestamp
      .make(GenericRow.genericRow("col2"), 987865L);

  @Mock
  private KsStateStore stateStore;
  @Mock
  private ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> tableStore;
  @Mock
  private WindowStoreIterator<ValueAndTimestamp<GenericRow>> fetchIterator;
  @Mock
  private KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>> keyValueIterator;
  @Captor
  private ArgumentCaptor<QueryableStoreType<?>> storeTypeCaptor;
  @Mock
  private WindowStoreCacheBypassFetcher cacheBypassFetcher;
  @Mock
  private WindowStoreCacheBypassFetcherAll cacheBypassFetcherAll;
  @Mock
  private WindowStoreCacheBypassFetcherRange cacheBypassFetcherRange;

  private KsMaterializedWindowTable table;

  @Before
  public void setUp() {
    table = new KsMaterializedWindowTable(stateStore, WINDOW_SIZE,
            cacheBypassFetcher, cacheBypassFetcherAll, cacheBypassFetcherRange);

    when(stateStore.store(any(), anyInt())).thenReturn(tableStore);
    when(stateStore.schema()).thenReturn(SCHEMA);
    when(cacheBypassFetcher.fetch(any(), any(), any(), any())).thenReturn(fetchIterator);
    when(cacheBypassFetcherAll.fetchAll(any(), any(), any())).thenReturn(keyValueIterator);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(KsStateStore.class, stateStore)
        .testConstructors(KsMaterializedWindowTable.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldThrowIfGettingStateStoreFails() {
    // Given:
    when(stateStore.store(any(), anyInt())).thenThrow(new MaterializationTimeOutException("Boom"));

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Failed to get value from materialized table"));
    assertThat(e.getCause(), (instanceOf(MaterializationTimeOutException.class)));
  }

  @Test
  public void shouldThrowIfGettingStateStoreFails_fetchAll() {
    // Given:
    when(stateStore.store(any(), anyInt())).thenThrow(new MaterializationTimeOutException("Boom"));

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Failed to scan materialized table"));
    assertThat(e.getCause(), (instanceOf(MaterializationTimeOutException.class)));
  }

  @Test
  public void shouldThrowIfStoreFetchFails() {
    // Given:
    when(cacheBypassFetcher.fetch(any(), any(), any(), any()))
        .thenThrow(new MaterializationTimeOutException("Boom"));

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Failed to get value from materialized table"));
    assertThat(e.getCause(), (instanceOf(MaterializationTimeOutException.class)));
  }

  @Test
  public void shouldThrowIfStoreFetchFails_fetchAll() {
    // Given:
    when(cacheBypassFetcherAll.fetchAll(any(), any(), any()))
        .thenThrow(new MaterializationTimeOutException("Boom"));

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Failed to scan materialized table"));
    assertThat(e.getCause(), (instanceOf(MaterializationTimeOutException.class)));
  }

  @Test
  public void shouldGetStoreWithCorrectParams() {
    // When:
    table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    verify(stateStore).store(storeTypeCaptor.capture(), anyInt());
    assertThat(storeTypeCaptor.getValue().getClass().getSimpleName(),
        is("TimestampedWindowStoreType"));
  }

  @Test
  public void shouldGetStoreWithCorrectParams_fetchAll() {
    // When:
    table.get(PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    verify(stateStore).store(storeTypeCaptor.capture(), anyInt());
    assertThat(storeTypeCaptor.getValue().getClass().getSimpleName(),
        is("TimestampedWindowStoreType"));
  }

  @Test
  public void shouldFetchWithCorrectKey() {
    // Given:
    // When:
    table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    verify(cacheBypassFetcher).fetch(eq(tableStore), eq(A_KEY), any(), any());
  }

  @Test
  public void shouldFetchWithNoBounds() {
    // When:
    table.get(A_KEY, PARTITION, Range.all(), Range.all());

    // Then:
    verify(cacheBypassFetcher).fetch(
        eq(tableStore),
        any(),
        eq(Instant.ofEpochMilli(0)),
        eq(Instant.ofEpochMilli(Long.MAX_VALUE))
    );
  }

  @Test
  public void shouldFetchWithNoBounds_fetchAll() {
    // When:
    table.get(PARTITION, Range.all(), Range.all());

    // Then:
    verify(cacheBypassFetcherAll).fetchAll(
        eq(tableStore),
        eq(Instant.ofEpochMilli(0)),
        eq(Instant.ofEpochMilli(Long.MAX_VALUE))
    );
  }

  @Test
  public void shouldFetchWithOnlyStartBounds() {
    // When:
    table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, Range.all());

    // Then:
    verify(cacheBypassFetcher).fetch(
        eq(tableStore),
        any(),
        eq(WINDOW_START_BOUNDS.lowerEndpoint()),
        eq(WINDOW_START_BOUNDS.upperEndpoint())
    );
  }

  @Test
  public void shouldFetchWithOnlyStartBounds_fetchAll() {
    // When:
    table.get(PARTITION, WINDOW_START_BOUNDS, Range.all());

    // Then:
    verify(cacheBypassFetcherAll).fetchAll(
        eq(tableStore),
        eq(WINDOW_START_BOUNDS.lowerEndpoint()),
        eq(WINDOW_START_BOUNDS.upperEndpoint())
    );
  }

  @Test
  public void shouldFetchWithOnlyEndBounds() {
    // When:
    table.get(A_KEY, PARTITION, Range.all(), WINDOW_END_BOUNDS);

    // Then:
    verify(cacheBypassFetcher).fetch(
        eq(tableStore),
        any(),
        eq(WINDOW_END_BOUNDS.lowerEndpoint().minus(WINDOW_SIZE)),
        eq(WINDOW_END_BOUNDS.upperEndpoint().minus(WINDOW_SIZE))
    );
  }

  @Test
  public void shouldFetchWithOnlyEndBounds_fetchAll() {
    // When:
    table.get(PARTITION, Range.all(), WINDOW_END_BOUNDS);

    // Then:
    verify(cacheBypassFetcherAll).fetchAll(
        eq(tableStore),
        eq(WINDOW_END_BOUNDS.lowerEndpoint().minus(WINDOW_SIZE)),
        eq(WINDOW_END_BOUNDS.upperEndpoint().minus(WINDOW_SIZE))
    );
  }

  @Test
  public void shouldFetchWithStartLowerBoundIfHighest() {
    // Given:
    final Range<Instant> startBounds = Range.closed(
        NOW.plusSeconds(5),
        NOW.plusSeconds(10)
    );

    final Range<Instant> endBounds = Range.closed(
        NOW,
        NOW.plusSeconds(15).plus(WINDOW_SIZE)
    );

    // When:
    table.get(A_KEY, PARTITION, startBounds, endBounds);

    // Then:
    verify(cacheBypassFetcher).fetch(eq(tableStore), any(), eq(startBounds.lowerEndpoint()),
        any());
  }

  @Test
  public void shouldFetchWithEndLowerBoundIfHighest() {
    // Given:
    final Range<Instant> startBounds = Range.closed(
        NOW,
        NOW.plusSeconds(10)
    );

    final Range<Instant> endBounds = Range.closed(
        NOW.plusSeconds(5).plus(WINDOW_SIZE),
        NOW.plusSeconds(15).plus(WINDOW_SIZE)
    );

    // When:
    table.get(A_KEY, PARTITION, startBounds, endBounds);

    // Then:
    verify(cacheBypassFetcher).fetch(eq(tableStore), any(),
        eq(endBounds.lowerEndpoint().minus(WINDOW_SIZE)), any());
  }

  @Test
  public void shouldFetchWithStartUpperBoundIfLowest() {
    // Given:
    final Range<Instant> startBounds = Range.closed(
        NOW,
        NOW.plusSeconds(10)
    );

    final Range<Instant> endBounds = Range.closed(
        NOW.plusSeconds(5).plus(WINDOW_SIZE),
        NOW.plusSeconds(15).plus(WINDOW_SIZE)
    );

    // When:
    table.get(A_KEY, PARTITION, startBounds, endBounds);

    // Then:
    verify(cacheBypassFetcher).fetch(eq(tableStore), any(), any(), eq(startBounds.upperEndpoint()));
  }

  @Test
  public void shouldFetchWithEndUpperBoundIfLowest() {
    // Given:
    final Range<Instant> startBounds = Range.closed(
        NOW,
        NOW.plusSeconds(20)
    );

    final Range<Instant> endBounds = Range.closed(
        NOW.plusSeconds(5),
        NOW.plusSeconds(10)
    );

    // When:
    table.get(A_KEY, PARTITION, startBounds, endBounds);

    // Then:
    verify(cacheBypassFetcher).fetch(
        eq(tableStore), any(), any(), eq(endBounds.upperEndpoint().minus(WINDOW_SIZE)));
  }

  @Test
  public void shouldCloseIterator() {
    // When:
    table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    verify(fetchIterator).close();
  }

  @Test
  public void shouldCloseIterator_fetchAll() {
    // When:
    Streams.stream((table.get(PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS).getRowIterator()))
        .collect(Collectors.toList());

    // Then:
    verify(keyValueIterator).close();
  }

  @Test
  public void shouldReturnEmptyIfKeyNotPresent() {
    // When:
    final Iterator<WindowedRow> rowIterator =
        table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS).rowIterator;

    // Then:
    assertThat(rowIterator.hasNext(), is(false));
  }

  @Test
  public void shouldReturnValuesForClosedStartBounds() {
    // Given:
    final Range<Instant> start = Range.closed(
        NOW,
        NOW.plusSeconds(10)
    );

    when(fetchIterator.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    when(fetchIterator.next())
        .thenReturn(new KeyValue<>(start.lowerEndpoint().toEpochMilli(), VALUE_1))
        .thenReturn(new KeyValue<>(start.upperEndpoint().toEpochMilli(), VALUE_2))
        .thenThrow(new AssertionError());

    when(cacheBypassFetcher.fetch(eq(tableStore), any(), any(), any())).thenReturn(fetchIterator);

    // When:
    final Iterator<WindowedRow> rowIterator =
        table.get(A_KEY, PARTITION, start, Range.all()).rowIterator;

    // Then:
    assertThat(rowIterator.hasNext(), is(true));
    final List<WindowedRow> resultList = Lists.newArrayList(rowIterator);
    assertThat(resultList, contains(
        WindowedRow.of(
            SCHEMA,
            windowedKey(start.lowerEndpoint()),
            VALUE_1.value(),
            VALUE_1.timestamp()
        ),
        WindowedRow.of(
            SCHEMA,
            windowedKey(start.upperEndpoint()),
            VALUE_2.value(),
            VALUE_2.timestamp()
        )
    ));
  }

  @Test
  public void shouldReturnValuesForClosedStartBounds_fetchAll() {
    // Given:
    final Range<Instant> start = Range.closed(
        NOW,
        NOW.plusSeconds(10)
    );

    when(keyValueIterator.hasNext())
        .thenReturn(true, true, false);

    when(keyValueIterator.next())
        .thenReturn(new KeyValue<>(new Windowed<>(A_KEY,
            new TimeWindow(start.lowerEndpoint().toEpochMilli(),
                start.lowerEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_1))
        .thenReturn(new KeyValue<>(new Windowed<>(A_KEY2,
            new TimeWindow(start.upperEndpoint().toEpochMilli(),
                start.upperEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_2))
        .thenThrow(new AssertionError());


    // When:
    final Iterator<WindowedRow> rowIterator =
        table.get(PARTITION, start, Range.all()).rowIterator;

    // Then:
    
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(),
        is (WindowedRow.of(
            SCHEMA,
            windowedKey(start.lowerEndpoint()),
            VALUE_1.value(),
            VALUE_1.timestamp())));
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(),
        is(WindowedRow.of(
            SCHEMA,
            windowedKey(A_KEY2, start.upperEndpoint()),
            VALUE_2.value(),
            VALUE_2.timestamp())));
    assertThat(rowIterator.hasNext(), is(false));
  }

  @Test
  public void shouldReturnValuesForClosedEndBounds() {
    // Given:
    final Range<Instant> end = Range.closed(
        NOW,
        NOW.plusSeconds(10)
    );

    final Range<Instant> startEqiv = Range.closed(
        end.lowerEndpoint().minus(WINDOW_SIZE),
        end.lowerEndpoint().minus(WINDOW_SIZE)
    );

    when(fetchIterator.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    when(fetchIterator.next())
        .thenReturn(new KeyValue<>(startEqiv.lowerEndpoint().toEpochMilli(), VALUE_1))
        .thenReturn(new KeyValue<>(startEqiv.upperEndpoint().toEpochMilli(), VALUE_2))
        .thenThrow(new AssertionError());

    when(cacheBypassFetcher.fetch(eq(tableStore), any(), any(), any())).thenReturn(fetchIterator);

    // When:
    final Iterator<WindowedRow> rowIterator =
        table.get(A_KEY, PARTITION, Range.all(), end).rowIterator;

    // Then:
    
    assertThat(rowIterator.hasNext(), is(true));
    final List<WindowedRow> resultList = Lists.newArrayList(rowIterator);
    assertThat(resultList, contains(
        WindowedRow.of(
            SCHEMA,
            windowedKey(startEqiv.lowerEndpoint()),
            VALUE_1.value(),
            VALUE_1.timestamp()
        ),
        WindowedRow.of(
            SCHEMA,
            windowedKey(startEqiv.upperEndpoint()),
            VALUE_2.value(),
            VALUE_2.timestamp()
        )
    ));
  }

  @Test
  public void shouldReturnValuesForClosedEndBounds_fetchAll() {
    // Given:
    final Range<Instant> end = Range.closed(
        NOW,
        NOW.plusSeconds(10)
    );

    final Range<Instant> startEqiv = Range.closed(
        end.lowerEndpoint().minus(WINDOW_SIZE),
        end.lowerEndpoint().minus(WINDOW_SIZE)
    );

    when(keyValueIterator.hasNext())
        .thenReturn(true, true, false);

    when(keyValueIterator.next())
        .thenReturn(new KeyValue<>(new Windowed<>(A_KEY,
            new TimeWindow(startEqiv.lowerEndpoint().toEpochMilli(),
                startEqiv.lowerEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_1))
        .thenReturn(new KeyValue<>(new Windowed<>(A_KEY2,
            new TimeWindow(startEqiv.upperEndpoint().toEpochMilli(),
                startEqiv.upperEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_2))
        .thenThrow(new AssertionError());


    // When:
    final Iterator<WindowedRow> rowIterator =
        table.get(PARTITION, Range.all(), end).rowIterator;

    // Then:
    
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(),
        is (WindowedRow.of(
            SCHEMA,
            windowedKey(startEqiv.lowerEndpoint()),
            VALUE_1.value(),
            VALUE_1.timestamp())));
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(),
        is(WindowedRow.of(
            SCHEMA,
            windowedKey(A_KEY2, startEqiv.upperEndpoint()),
            VALUE_2.value(),
            VALUE_2.timestamp())));
    assertThat(rowIterator.hasNext(), is(false));
  }

  @Test
  public void shouldReturnValuesForOpenStartBounds() {
    // Given:
    final Range<Instant> start = Range.open(
        NOW,
        NOW.plusSeconds(10)
    );

    when(fetchIterator.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    when(fetchIterator.next())
        .thenReturn(new KeyValue<>(start.lowerEndpoint().toEpochMilli(), VALUE_1))
        .thenReturn(new KeyValue<>(start.lowerEndpoint().plusMillis(1).toEpochMilli(), VALUE_2))
        .thenReturn(new KeyValue<>(start.upperEndpoint().toEpochMilli(), VALUE_3))
        .thenThrow(new AssertionError());

    when(cacheBypassFetcher.fetch(eq(tableStore), any(), any(), any())).thenReturn(fetchIterator);

    // When:
    final Iterator<WindowedRow> rowIterator =
        table.get(A_KEY, PARTITION, start, Range.all()).rowIterator;

    // Then:
    
    assertThat(rowIterator.hasNext(), is(true));
    final List<WindowedRow> resultList = Lists.newArrayList(rowIterator);
    assertThat(resultList, contains(
        WindowedRow.of(
            SCHEMA,
            windowedKey(start.lowerEndpoint().plusMillis(1)),
            VALUE_2.value(),
            VALUE_2.timestamp()
        )
    ));
  }

  @Test
  public void shouldReturnValuesForOpenStartBounds_fetchAll() {
    // Given:
    final Range<Instant> start = Range.open(
        NOW,
        NOW.plusSeconds(10)
    );

    when(keyValueIterator.hasNext())
        .thenReturn(true, true, true, false);

    when(keyValueIterator.next())
        .thenReturn(new KeyValue<>(new Windowed<>(A_KEY,
            new TimeWindow(start.lowerEndpoint().toEpochMilli(),
                start.lowerEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_1))
        .thenReturn(new KeyValue<>(new Windowed<>(A_KEY2,
            new TimeWindow(start.lowerEndpoint().plusMillis(1).toEpochMilli(),
                start.lowerEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis() + 1)), VALUE_2))
        .thenReturn(new KeyValue<>(new Windowed<>(A_KEY3,
            new TimeWindow(start.upperEndpoint().toEpochMilli(),
                start.upperEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_3))
        .thenThrow(new AssertionError());


    // When:
    final Iterator<WindowedRow> rowIterator =
        table.get(PARTITION, start, Range.all()).rowIterator;

    // Then:
    
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(),
        is (WindowedRow.of(
            SCHEMA,
            windowedKey(A_KEY2, start.lowerEndpoint().plusMillis(1)),
            VALUE_2.value(),
            VALUE_2.timestamp())));
    assertThat(rowIterator.hasNext(), is(false));
  }

  @Test
  public void shouldReturnValuesForOpenEndBounds() {
    // Given:
    final Range<Instant> end = Range.open(
        NOW,
        NOW.plusSeconds(10)
    );

    final Range<Instant> startEquiv = Range.open(
        end.lowerEndpoint().minus(WINDOW_SIZE),
        end.upperEndpoint().minus(WINDOW_SIZE)
    );

    when(fetchIterator.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    when(fetchIterator.next())
        .thenReturn(new KeyValue<>(startEquiv.lowerEndpoint().toEpochMilli(), VALUE_1))
        .thenReturn(
            new KeyValue<>(startEquiv.lowerEndpoint().plusMillis(1).toEpochMilli(), VALUE_2))
        .thenReturn(new KeyValue<>(startEquiv.upperEndpoint().toEpochMilli(), VALUE_3))
        .thenThrow(new AssertionError());

    when(cacheBypassFetcher.fetch(eq(tableStore), any(), any(), any())).thenReturn(fetchIterator);

    // When:
    final Iterator<WindowedRow> rowIterator =
        table.get(A_KEY, PARTITION, Range.all(), end).rowIterator;

    // Then:
    
    assertThat(rowIterator.hasNext(), is(true));
    final List<WindowedRow> resultList = Lists.newArrayList(rowIterator);
    assertThat(resultList, contains(
        WindowedRow.of(
            SCHEMA,
            windowedKey(startEquiv.lowerEndpoint().plusMillis(1)),
            VALUE_2.value(),
            VALUE_2.timestamp()
        )
    ));
  }

  @Test
  public void shouldReturnValuesForOpenEndBounds_fetchAll() {
    // Given:
    final Range<Instant> end = Range.open(
        NOW,
        NOW.plusSeconds(10)
    );

    final Range<Instant> startEquiv = Range.open(
        end.lowerEndpoint().minus(WINDOW_SIZE),
        end.upperEndpoint().minus(WINDOW_SIZE)
    );

    when(keyValueIterator.hasNext())
        .thenReturn(true, true, true, false);

    when(keyValueIterator.next())
        .thenReturn(new KeyValue<>(new Windowed<>(A_KEY,
            new TimeWindow(startEquiv.lowerEndpoint().toEpochMilli(),
                startEquiv.lowerEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_1))
        .thenReturn(new KeyValue<>(new Windowed<>(A_KEY2,
            new TimeWindow(startEquiv.lowerEndpoint().plusMillis(1).toEpochMilli(),
                startEquiv.lowerEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis() + 1)), VALUE_2))
        .thenReturn(new KeyValue<>(new Windowed<>(A_KEY3,
            new TimeWindow(startEquiv.upperEndpoint().toEpochMilli(),
                startEquiv.upperEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_3))
        .thenThrow(new AssertionError());


    // When:
    final Iterator<WindowedRow> rowIterator =
        table.get(PARTITION, Range.all(), end).rowIterator;

    // Then:
    
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(),
        is (WindowedRow.of(
            SCHEMA,
            windowedKey(A_KEY2, startEquiv.lowerEndpoint().plusMillis(1)),
            VALUE_2.value(),
            VALUE_2.timestamp())));
    assertThat(rowIterator.hasNext(), is(false));
  }

  @Test
  public void shouldMaintainResultOrder() {
    // Given:
    when(fetchIterator.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    final Instant start = WINDOW_START_BOUNDS.lowerEndpoint();

    when(fetchIterator.next())
        .thenReturn(new KeyValue<>(start.toEpochMilli(), VALUE_1))
        .thenReturn(new KeyValue<>(start.plusMillis(1).toEpochMilli(), VALUE_2))
        .thenReturn(new KeyValue<>(start.plusMillis(2).toEpochMilli(), VALUE_3))
        .thenThrow(new AssertionError());

    when(cacheBypassFetcher.fetch(eq(tableStore), any(), any(), any())).thenReturn(fetchIterator);

    // When:
    final Iterator<WindowedRow> rowIterator =
        table.get(A_KEY, PARTITION, Range.all(), Range.all()).rowIterator;

    // Then:
    
    assertThat(rowIterator.hasNext(), is(true));
    final List<WindowedRow> resultList = Lists.newArrayList(rowIterator);
    assertThat(resultList, contains(
        WindowedRow.of(
            SCHEMA,
            windowedKey(start),
            VALUE_1.value(),
            VALUE_1.timestamp()
        ),
        WindowedRow.of(
            SCHEMA,
            windowedKey(start.plusMillis(1)),
            VALUE_2.value(),
            VALUE_2.timestamp()
        ),
        WindowedRow.of(
            SCHEMA,
            windowedKey(start.plusMillis(2)),
            VALUE_3.value(),
            VALUE_3.timestamp()
        )
    ));
  }

  @Test
  public void shouldSupportRangeAll() {
    // When:
    table.get(A_KEY, PARTITION, Range.all(), Range.all());

    // Then:
    verify(cacheBypassFetcher).fetch(
        tableStore,
        A_KEY,
        Instant.ofEpochMilli(0L),
        Instant.ofEpochMilli(Long.MAX_VALUE)
    );
  }

  @Test
  public void shouldSupportRangeAll_fetchAll() {
    // When:
    table.get(PARTITION, Range.all(), Range.all());

    // Then:
    verify(cacheBypassFetcherAll).fetchAll(
        tableStore,
        Instant.ofEpochMilli(0L),
        Instant.ofEpochMilli(Long.MAX_VALUE)
    );
  }

  private static Windowed<GenericKey> windowedKey(final Instant windowStart) {
    return windowedKey(A_KEY, windowStart);
  }

  private static Windowed<GenericKey> windowedKey(GenericKey key, final Instant windowStart) {
    return new Windowed<>(
        key,
        new TimeWindow(windowStart.toEpochMilli(), windowStart.plus(WINDOW_SIZE).toEpochMilli())
    );
  }
}