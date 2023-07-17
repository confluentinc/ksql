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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.ValueAndTimestamp;
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

  @Mock
  private QueryableStoreType<ReadOnlySessionStore<GenericKey, GenericRow>>
      queryableStoreType;
  @Mock
  private StateStoreProvider provider;
  @Mock
  private MeteredSessionStore<GenericKey, GenericRow> meteredSessionStore;
  @Mock
  private SessionStore<Bytes, byte[]> sessionStore;
  @Mock
  private WrappedSessionStore<Bytes, byte[]> wrappedSessionStore;
  @Mock
  private StateStore stateStore;
  @Mock
  private KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator;
  @Mock
  private StateSerdes<GenericKey, ValueAndTimestamp<GenericRow>> serdes;

  private CompositeReadOnlySessionStore<GenericKey, GenericRow> store;

  @Before
  public void setUp() {
    store = new CompositeReadOnlySessionStore<>(provider, queryableStoreType, "foo");
  }

  @Test
  public void shouldCallUnderlyingStoreSingleKey() throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredSessionStore));
    SERDES_FIELD.set(meteredSessionStore, serdes);
    when(serdes.rawKey(any())).thenReturn(BYTES);
    when(meteredSessionStore.wrapped()).thenReturn(wrappedSessionStore);
    when(wrappedSessionStore.wrapped()).thenReturn(sessionStore);
    when(sessionStore.fetch(any())).thenReturn(storeIterator);
    when(storeIterator.hasNext()).thenReturn(false);

    SessionStoreCacheBypass.fetch(store, SOME_KEY);
    verify(sessionStore).fetch(new Bytes(BYTES));
  }

  @Test
  public void shouldCallUnderlyingStoreRangeQuery() throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredSessionStore));
    SERDES_FIELD.set(meteredSessionStore, serdes);
    when(serdes.rawKey(any())).thenReturn(BYTES, OTHER_BYTES);
    when(meteredSessionStore.wrapped()).thenReturn(wrappedSessionStore);
    when(wrappedSessionStore.wrapped()).thenReturn(sessionStore);
    when(sessionStore.fetch(any(), any())).thenReturn(storeIterator);
    when(storeIterator.hasNext()).thenReturn(false);

    SessionStoreCacheBypass.fetchRange(store, SOME_KEY, SOME_OTHER_KEY);
    verify(sessionStore).fetch(new Bytes(BYTES), new Bytes(OTHER_BYTES));
  }

  @Test
  public void shouldAvoidNonSessionStore() throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredSessionStore));
    SERDES_FIELD.set(meteredSessionStore, serdes);
    when(serdes.rawKey(any())).thenReturn(BYTES);
    when(meteredSessionStore.wrapped()).thenReturn(wrappedSessionStore);
    when(wrappedSessionStore.wrapped()).thenReturn(stateStore);
    when(wrappedSessionStore.fetch(any())).thenReturn(storeIterator);
    when(storeIterator.hasNext()).thenReturn(false);

    SessionStoreCacheBypass.fetch(store, SOME_KEY);
    verify(wrappedSessionStore).fetch(new Bytes(BYTES));
  }

  @Test
  public void shouldThrowException_InvalidStateStoreException() throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredSessionStore));
    SERDES_FIELD.set(meteredSessionStore, serdes);
    when(serdes.rawKey(any())).thenReturn(BYTES);
    when(meteredSessionStore.wrapped()).thenReturn(sessionStore);
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
  public void shouldThrowException_wrongStateStore() {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(sessionStore));

    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> SessionStoreCacheBypass.fetch(store, SOME_KEY)
    );

    assertThat(e.getMessage(), containsString("Expecting a MeteredSessionStore"));
  }

  private static abstract class WrappedSessionStore<K, V>
      extends WrappedStateStore<StateStore, K, V> implements SessionStore<K, V> {
    public WrappedSessionStore(StateStore wrapped) {
      super(wrapped);
    }
  }
}
