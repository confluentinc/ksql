/*
 * Copyright 2020 Confluent Inc.
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

import static io.confluent.ksql.execution.streams.materialization.ks.WindowStoreCacheBypass.SERDES_FIELD;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import java.time.Instant;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.MeteredWindowStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WindowStoreCacheBypassTest {

  private static final GenericKey SOME_KEY = GenericKey.genericKey(1);
  private static final GenericKey SOME_OTHER_KEY = GenericKey.genericKey(2);
  private static final byte[] BYTES = new byte[] {'a', 'b'};
  private static final byte[] OTHER_BYTES = new byte[] {'c', 'd'};

  @Mock
  private QueryableStoreType<ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>>>
      queryableStoreType;
  @Mock
  private StateStoreProvider provider;
  @Mock
  private MeteredWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> meteredWindowStore;
  @Mock
  private WindowStore<Bytes, byte[]> windowStore;
  @Mock
  private WrappedWindowStore<Bytes, byte[]> wrappedWindowStore;
  @Mock
  private StateStore stateStore;
  @Mock
  private WindowStoreIterator<byte[]> windowStoreIterator;
  @Mock
  private KeyValueIterator<Windowed<Bytes>, byte[]> keyValueIterator;
  @Mock
  private StateSerdes<GenericKey, ValueAndTimestamp<GenericRow>> serdes;

  private CompositeReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> store;

  @Before
  public void setUp() {
    store = new CompositeReadOnlyWindowStore<>(provider, queryableStoreType, "foo");
  }

  @Test
  public void shouldCallUnderlyingStoreSingleKey() throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredWindowStore));
    SERDES_FIELD.set(meteredWindowStore, serdes);
    when(serdes.rawKey(any())).thenReturn(BYTES);
    when(meteredWindowStore.wrapped()).thenReturn(wrappedWindowStore);
    when(wrappedWindowStore.wrapped()).thenReturn(windowStore);
    when(windowStore.fetch(any(), any(), any())).thenReturn(windowStoreIterator);
    when(windowStoreIterator.hasNext()).thenReturn(false);

    WindowStoreCacheBypass.fetch(store, SOME_KEY, Instant.ofEpochMilli(100), Instant.ofEpochMilli(200));
    verify(windowStore).fetch(new Bytes(BYTES), Instant.ofEpochMilli(100L), Instant.ofEpochMilli(200L));
  }

  @Test
  public void shouldCallUnderlyingStoreForRangeQuery() throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredWindowStore));
    SERDES_FIELD.set(meteredWindowStore, serdes);
    when(serdes.rawKey(any())).thenReturn(BYTES, OTHER_BYTES);
    when(meteredWindowStore.wrapped()).thenReturn(wrappedWindowStore);
    when(wrappedWindowStore.wrapped()).thenReturn(windowStore);
    when(windowStore.fetch(any(), any(), any(), any())).thenReturn(keyValueIterator);
    when(keyValueIterator.hasNext()).thenReturn(false);

    WindowStoreCacheBypass.fetchRange(store, SOME_KEY, SOME_OTHER_KEY, Instant.ofEpochMilli(100), Instant.ofEpochMilli(200));
    verify(windowStore).fetch(new Bytes(BYTES), new Bytes(OTHER_BYTES), Instant.ofEpochMilli(100L), Instant.ofEpochMilli(200L));
  }

  @Test
  public void shouldCallUnderlyingStoreForTableScans() throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredWindowStore));
    SERDES_FIELD.set(meteredWindowStore, serdes);
    when(meteredWindowStore.wrapped()).thenReturn(wrappedWindowStore);
    when(wrappedWindowStore.wrapped()).thenReturn(windowStore);
    when(windowStore.fetchAll(any(), any())).thenReturn(keyValueIterator);
    when(keyValueIterator.hasNext()).thenReturn(false);

    WindowStoreCacheBypass.fetchAll(store, Instant.ofEpochMilli(100), Instant.ofEpochMilli(200));
    verify(windowStore).fetchAll(Instant.ofEpochMilli(100L), Instant.ofEpochMilli(200L));
  }

  @Test
  public void shouldAvoidNonWindowStore() throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredWindowStore));
    SERDES_FIELD.set(meteredWindowStore, serdes);
    when(serdes.rawKey(any())).thenReturn(BYTES);
    when(meteredWindowStore.wrapped()).thenReturn(wrappedWindowStore);
    when(wrappedWindowStore.wrapped()).thenReturn(stateStore);
    when(wrappedWindowStore.fetch(any(), any(), any())).thenReturn(windowStoreIterator);
    when(windowStoreIterator.hasNext()).thenReturn(false);

    WindowStoreCacheBypass.fetch(
        store, SOME_KEY, Instant.ofEpochMilli(100), Instant.ofEpochMilli(200));
    verify(wrappedWindowStore).fetch(
        new Bytes(BYTES), Instant.ofEpochMilli(100L), Instant.ofEpochMilli(200L));
  }

  @Test
  public void shouldThrowException_InvalidStateStoreException() throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredWindowStore));
    SERDES_FIELD.set(meteredWindowStore, serdes);
    when(serdes.rawKey(any())).thenReturn(BYTES);
    when(meteredWindowStore.wrapped()).thenReturn(windowStore);
    when(windowStore.fetch(any(), any(), any())).thenThrow(
        new InvalidStateStoreException("Invalid"));

    final Exception e = assertThrows(
        InvalidStateStoreException.class,
        () -> WindowStoreCacheBypass.fetch(store, SOME_KEY,
            Instant.ofEpochMilli(100), Instant.ofEpochMilli(200))
    );

    assertThat(e.getMessage(), containsString("State store is not "
        + "available anymore and may have been migrated to another instance"));
  }

  @Test
  public void shouldThrowException_wrongStateStore() {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(windowStore));

    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> WindowStoreCacheBypass.fetch(store, SOME_KEY,
            Instant.ofEpochMilli(100), Instant.ofEpochMilli(200))
    );

    assertThat(e.getMessage(), containsString("Expecting a MeteredWindowStore"));
  }

  private static abstract class WrappedWindowStore<K, V>
      extends WrappedStateStore<StateStore, K, V> implements WindowStore<K, V> {
    public WrappedWindowStore(StateStore wrapped) {
      super(wrapped);
    }
  }
}
