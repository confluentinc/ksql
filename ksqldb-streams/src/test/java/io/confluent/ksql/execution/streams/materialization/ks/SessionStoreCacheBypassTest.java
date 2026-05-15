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

import static io.confluent.ksql.execution.streams.materialization.ks.SessionStoreCacheBypass.SERDES_FIELD;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.internals.MeteredSessionStoreWithHeaders;
import org.apache.kafka.streams.state.internals.ReadOnlySessionStoreFacade;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.CompositeReadOnlySessionStore;
import org.apache.kafka.streams.state.internals.MeteredSessionStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SessionStoreCacheBypassTest {

  private static final GenericKey SOME_KEY = GenericKey.genericKey(1);
  private static final GenericKey SOME_OTHER_KEY = GenericKey.genericKey(2);

  private static final byte[] BYTES = new byte[] {'a', 'b'};
  private static final byte[] OTHER_BYTES = new byte[] {'c', 'd'};
  private static final byte[] VALUE_BYTES = new byte[] {'e', 'f'};
  private static final GenericRow EXPECTED_ROW = GenericRow.genericRow("v1");

  @Mock
  private QueryableStoreType<ReadOnlySessionStore<GenericKey, GenericRow>>
      queryableStoreType;
  @Mock
  private StateStoreProvider provider;
  @Mock
  private SessionStore<Bytes, byte[]> sessionStore;
  @Mock
  private WrappedSessionStore<Bytes, byte[]> wrappedSessionStore;
  @Mock
  private StateStore stateStore;
  @Mock
  private KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator;
  @Mock
  private StateSerdes<GenericKey, AggregationWithHeaders<GenericRow>> serdes;
  @Mock
  private MeteredSessionStoreWithHeaders<GenericKey, GenericRow> meteredSessionStoreWithHeaders;
  @Mock
  private MeteredSessionStore<GenericKey, GenericRow> meteredSessionStore;
  @Mock
  private StateSerdes<GenericKey, GenericRow> plainSerdes;

  private CompositeReadOnlySessionStore<GenericKey, GenericRow> store;

  @Before
  public void setUp() {
    store = new CompositeReadOnlySessionStore<>(provider, queryableStoreType, "foo");
  }

  @Test
  public void shouldCallUnderlyingStoreSingleKey() throws IllegalAccessException {
    final TestSessionStoreFacade facade =
        new TestSessionStoreFacade(meteredSessionStoreWithHeaders);
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(facade));
    SERDES_FIELD.set(meteredSessionStoreWithHeaders, serdes);
    when(serdes.rawKey(any(), any())).thenReturn(BYTES);
    when(meteredSessionStoreWithHeaders.wrapped()).thenReturn(wrappedSessionStore);
    when(wrappedSessionStore.wrapped()).thenReturn(sessionStore);
    when(sessionStore.fetch(any())).thenReturn(storeIterator);
    when(storeIterator.hasNext()).thenReturn(false);

    SessionStoreCacheBypass.fetch(store, SOME_KEY);
    verify(sessionStore).fetch(new Bytes(BYTES));
  }

  @Test
  public void shouldCallUnderlyingStoreRangeQuery() throws IllegalAccessException {
    final TestSessionStoreFacade facade =
        new TestSessionStoreFacade(meteredSessionStoreWithHeaders);
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(facade));
    SERDES_FIELD.set(meteredSessionStoreWithHeaders, serdes);
    when(serdes.rawKey(any(), any())).thenReturn(BYTES, OTHER_BYTES);
    when(meteredSessionStoreWithHeaders.wrapped()).thenReturn(wrappedSessionStore);
    when(wrappedSessionStore.wrapped()).thenReturn(sessionStore);
    when(sessionStore.fetch(any(), any())).thenReturn(storeIterator);
    when(storeIterator.hasNext()).thenReturn(false);

    SessionStoreCacheBypass.fetchRange(store, SOME_KEY, SOME_OTHER_KEY);
    verify(sessionStore).fetch(new Bytes(BYTES), new Bytes(OTHER_BYTES));
  }

  @Test
  public void shouldAvoidNonSessionStore() throws IllegalAccessException {
    final TestSessionStoreFacade facade =
        new TestSessionStoreFacade(meteredSessionStoreWithHeaders);
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(facade));
    SERDES_FIELD.set(meteredSessionStoreWithHeaders, serdes);
    when(serdes.rawKey(any(), any())).thenReturn(BYTES);
    when(meteredSessionStoreWithHeaders.wrapped()).thenReturn(wrappedSessionStore);
    when(wrappedSessionStore.wrapped()).thenReturn(stateStore);
    when(wrappedSessionStore.fetch(any())).thenReturn(storeIterator);
    when(storeIterator.hasNext()).thenReturn(false);

    SessionStoreCacheBypass.fetch(store, SOME_KEY);
    verify(wrappedSessionStore).fetch(new Bytes(BYTES));
  }

  @Test
  public void shouldThrowException_InvalidStateStoreException() throws IllegalAccessException {
    final TestSessionStoreFacade facade =
        new TestSessionStoreFacade(meteredSessionStoreWithHeaders);
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(facade));
    SERDES_FIELD.set(meteredSessionStoreWithHeaders, serdes);
    when(serdes.rawKey(any(), any())).thenReturn(BYTES);
    when(meteredSessionStoreWithHeaders.wrapped()).thenReturn(sessionStore);
    when(sessionStore.fetch(any())).thenThrow(
        new InvalidStateStoreException("Invalid"));

    final Exception e = assertThrows(
        InvalidStateStoreException.class,
        () -> SessionStoreCacheBypass.fetch(store, SOME_KEY)
    );

    assertThat(e.getMessage(), containsString("State store is not "
        + "available anymore and may have been migrated to another instance"));
  }

  @Test
  public void shouldConvertValueViaAggregationMethod() throws IllegalAccessException {
    final TestSessionStoreFacade facade =
        new TestSessionStoreFacade(meteredSessionStoreWithHeaders);
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(facade));
    SERDES_FIELD.set(meteredSessionStoreWithHeaders, serdes);
    when(serdes.rawKey(any(), any())).thenReturn(BYTES);
    when(serdes.keyFrom(any())).thenReturn(SOME_KEY);
    final AggregationWithHeaders<GenericRow> aggregation =
        AggregationWithHeaders.make(EXPECTED_ROW, new RecordHeaders());
    doReturn(aggregation).when(serdes).valueFrom(any());
    when(meteredSessionStoreWithHeaders.wrapped()).thenReturn(wrappedSessionStore);
    when(wrappedSessionStore.wrapped()).thenReturn(sessionStore);
    final Windowed<Bytes> windowedKey =
        new Windowed<>(new Bytes(BYTES), new SessionWindow(0, 100));
    when(sessionStore.fetch(any())).thenReturn(storeIterator);
    when(storeIterator.hasNext()).thenReturn(true, false);
    when(storeIterator.peekNextKey()).thenReturn(windowedKey);
    when(storeIterator.next()).thenReturn(KeyValue.pair(windowedKey, VALUE_BYTES));

    final KeyValueIterator<Windowed<GenericKey>, GenericRow> result =
        SessionStoreCacheBypass.fetch(store, SOME_KEY);
    assertThat(result.next().value, is(EXPECTED_ROW));
  }

  // When the underlying store is a plain SessionStore (not WithHeaders), Kafka Streams'
  // validateAndCastStores returns it directly without wrapping in ReadOnlySessionStoreFacade.
  // The bypass must handle that shape too, deserializing values as GenericRow directly.
  @Test
  public void shouldCallUnderlyingStoreWhenProviderReturnsMeteredStoreDirectly()
      throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredSessionStore));
    SERDES_FIELD.set(meteredSessionStore, plainSerdes);
    when(plainSerdes.rawKey(any(), any())).thenReturn(BYTES);
    when(meteredSessionStore.wrapped()).thenReturn(wrappedSessionStore);
    when(wrappedSessionStore.wrapped()).thenReturn(sessionStore);
    when(sessionStore.fetch(any())).thenReturn(storeIterator);
    when(storeIterator.hasNext()).thenReturn(false);

    SessionStoreCacheBypass.fetch(store, SOME_KEY);
    verify(sessionStore).fetch(new Bytes(BYTES));
  }

  @Test
  public void shouldDeserializeGenericRowDirectlyWhenProviderReturnsMeteredStoreDirectly()
      throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredSessionStore));
    SERDES_FIELD.set(meteredSessionStore, plainSerdes);
    when(plainSerdes.rawKey(any(), any())).thenReturn(BYTES);
    when(plainSerdes.keyFrom(any())).thenReturn(SOME_KEY);
    doReturn(EXPECTED_ROW).when(plainSerdes).valueFrom(any());
    when(meteredSessionStore.wrapped()).thenReturn(wrappedSessionStore);
    when(wrappedSessionStore.wrapped()).thenReturn(sessionStore);
    final Windowed<Bytes> windowedKey =
        new Windowed<>(new Bytes(BYTES), new SessionWindow(0, 100));
    when(sessionStore.fetch(any())).thenReturn(storeIterator);
    when(storeIterator.hasNext()).thenReturn(true, false);
    when(storeIterator.peekNextKey()).thenReturn(windowedKey);
    when(storeIterator.next()).thenReturn(KeyValue.pair(windowedKey, VALUE_BYTES));

    final KeyValueIterator<Windowed<GenericKey>, GenericRow> result =
        SessionStoreCacheBypass.fetch(store, SOME_KEY);
    assertThat(result.next().value, is(EXPECTED_ROW));
  }

  private static abstract class WrappedSessionStore<K, V>
      extends WrappedStateStore<StateStore, K, V> implements SessionStore<K, V> {
    public WrappedSessionStore(StateStore wrapped) {
      super(wrapped);
    }
  }

  private static class TestSessionStoreFacade
      extends ReadOnlySessionStoreFacade<GenericKey, GenericRow> {
    TestSessionStoreFacade(final SessionStoreWithHeaders<GenericKey, GenericRow> inner) {
      super(inner);
    }
  }
}
