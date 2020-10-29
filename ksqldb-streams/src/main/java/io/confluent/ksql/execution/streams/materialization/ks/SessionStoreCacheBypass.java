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

import io.confluent.ksql.GenericRow;
import java.lang.reflect.Field;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.CompositeReadOnlySessionStore;
import org.apache.kafka.streams.state.internals.MeteredSessionStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

public final class SessionStoreCacheBypass {

  private static final Field PROVIDER_FIELD;
  private static final Field STORE_NAME_FIELD;
  private static final Field STORE_TYPE_FIELD;
  static final Field SERDES_FIELD;

  static {
    try {
      PROVIDER_FIELD = CompositeReadOnlySessionStore.class.getDeclaredField("storeProvider");
      PROVIDER_FIELD.setAccessible(true);
      STORE_NAME_FIELD = CompositeReadOnlySessionStore.class.getDeclaredField("storeName");
      STORE_NAME_FIELD.setAccessible(true);
      STORE_TYPE_FIELD
          = CompositeReadOnlySessionStore.class.getDeclaredField("queryableStoreType");
      STORE_TYPE_FIELD.setAccessible(true);
      SERDES_FIELD = MeteredSessionStore.class.getDeclaredField("serdes");
      SERDES_FIELD.setAccessible(true);
    } catch (final NoSuchFieldException e) {
      throw new RuntimeException("Stream internals changed unexpectedly!", e);
    }
  }

  private SessionStoreCacheBypass() {}

  interface SessionStoreCacheBypassFetcher {

    KeyValueIterator<Windowed<Struct>, GenericRow> fetch(
        ReadOnlySessionStore<Struct, GenericRow> store,
        Struct key
    );
  }

  @SuppressWarnings("unchecked")
  public static KeyValueIterator<Windowed<Struct>, GenericRow> fetch(
      final ReadOnlySessionStore<Struct, GenericRow> store,
      final Struct key
  ) {
    Objects.requireNonNull(key, "key can't be null");

    final StateStoreProvider provider;
    final String storeName;
    final QueryableStoreType<ReadOnlySessionStore<Struct, GenericRow>> storeType;
    try {
      provider = (StateStoreProvider) PROVIDER_FIELD.get(store);
      storeName = (String) STORE_NAME_FIELD.get(store);
      storeType = (QueryableStoreType<ReadOnlySessionStore<Struct, GenericRow>>)
          STORE_TYPE_FIELD.get(store);
    } catch (final IllegalAccessException e) {
      throw new RuntimeException("Stream internals changed unexpectedly!", e);
    }
    final List<ReadOnlySessionStore<Struct, GenericRow>> stores
        = provider.stores(storeName, storeType);
    for (final ReadOnlySessionStore<Struct, GenericRow> sessionStore : stores) {
      try {
        final KeyValueIterator<Windowed<Struct>, GenericRow> result
            = fetchUncached(sessionStore, key);
        // returns the first non-empty iterator
        if (!result.hasNext()) {
          result.close();
        } else {
          return result;
        }
      } catch (final InvalidStateStoreException e) {
        throw new InvalidStateStoreException(
            "State store is not available anymore and may have been migrated to another instance; "
                + "please re-discover its location from the state metadata.", e);
      }
    }
    return new EmptyKeyValueIterator();
  }

  @SuppressWarnings("unchecked")
  private static KeyValueIterator<Windowed<Struct>, GenericRow> fetchUncached(
      final ReadOnlySessionStore<Struct, GenericRow> sessionStore,
      final Struct key
  ) {
    if (sessionStore instanceof MeteredSessionStore) {
      final StateSerdes<Struct, GenericRow> serdes;
      try {
        serdes = (StateSerdes<Struct, GenericRow>) SERDES_FIELD.get(sessionStore);
      } catch (final IllegalAccessException e) {
        throw new RuntimeException("Stream internals changed unexpectedly!", e);
      }

      final Bytes rawKey = Bytes.wrap(serdes.rawKey(key));
      SessionStore<Bytes, byte[]> wrapped
          = ((MeteredSessionStore<Struct, GenericRow>) sessionStore).wrapped();
      while (wrapped instanceof WrappedStateStore) {
        final StateStore store = ((WrappedStateStore<?, ?, ?>) wrapped).wrapped();
        if (!(store instanceof SessionStore)) {
          break;
        }
        wrapped = (SessionStore<Bytes, byte[]>) store;
      }
      // now we have the innermost layer of the store.
      final KeyValueIterator<Windowed<Bytes>, byte[]> fetch = wrapped.fetch(rawKey);
      return new DeserializingIterator(fetch, serdes);
    } else {
      throw new IllegalStateException("Expecting a MeteredSessionStore");
    }
  }

  private static final class DeserializingIterator
      implements KeyValueIterator<Windowed<Struct>, GenericRow> {
    private final KeyValueIterator<Windowed<Bytes>, byte[]> fetch;
    private final StateSerdes<Struct, GenericRow> serdes;

    private DeserializingIterator(final KeyValueIterator<Windowed<Bytes>, byte[]> fetch,
        final StateSerdes<Struct, GenericRow> serdes) {
      this.fetch = fetch;
      this.serdes = serdes;
    }

    @Override
    public void close() {
      fetch.close();
    }

    @Override
    public Windowed<Struct> peekNextKey() {
      final Windowed<Bytes> windowed = fetch.peekNextKey();
      return new Windowed<>(serdes.keyFrom(windowed.key().get()), windowed.window());
    }

    @Override
    public boolean hasNext() {
      return fetch.hasNext();
    }

    @Override
    public KeyValue<Windowed<Struct>, GenericRow> next() {
      final Windowed<Struct> key = peekNextKey();
      final KeyValue<Windowed<Bytes>, byte[]> next = fetch.next();
      return KeyValue.pair(key, serdes.valueFrom(next.value));
    }
  }

  private static class EmptyKeyValueIterator
      implements KeyValueIterator<Windowed<Struct>, GenericRow> {

    @Override
    public void close() {
    }

    @Override
    public Windowed<Struct> peekNextKey() {
      throw new NoSuchElementException();
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public KeyValue<Windowed<Struct>, GenericRow> next() {
      throw new NoSuchElementException();
    }
  }
}
