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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import java.lang.reflect.Field;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.common.utils.Bytes;
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
  private static final String STORE_UNAVAILABLE_MESSAGE = "State store is not available anymore "
          + "and may have been migrated to another instance; "
          + "please re-discover its location from the state metadata.";

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

    KeyValueIterator<Windowed<GenericKey>, GenericRow> fetch(
        ReadOnlySessionStore<GenericKey, GenericRow> store,
        GenericKey key
    );
  }

  interface SessionStoreCacheBypassFetcherRange {

    KeyValueIterator<Windowed<GenericKey>, GenericRow> fetchRange(
            ReadOnlySessionStore<GenericKey, GenericRow> store,
            GenericKey keyFrom,
            GenericKey keyTo
    );
  }

  /*
  This method is used for single key lookups. It calls the fetchUncached method.
   */
  public static KeyValueIterator<Windowed<GenericKey>, GenericRow> fetch(
      final ReadOnlySessionStore<GenericKey, GenericRow> store,
      final GenericKey key
  ) {
    Objects.requireNonNull(key, "key can't be null");
    final List<ReadOnlySessionStore<GenericKey, GenericRow>> stores = getStores(store);
    final Function<ReadOnlySessionStore<GenericKey, GenericRow>,
            KeyValueIterator<Windowed<GenericKey>, GenericRow>> fetchFunc
            = sessionStore -> fetchUncached(sessionStore, key);
    return findFirstNonEmptyIterator(stores, fetchFunc);
  }

  /*
  This method is used for single key lookups. It is invoked by the fetch method
   */
  private static KeyValueIterator<Windowed<GenericKey>, GenericRow> fetchUncached(
      final ReadOnlySessionStore<GenericKey, GenericRow> sessionStore,
      final GenericKey key
  ) {
    if (!(sessionStore instanceof MeteredSessionStore)) {
      throw new IllegalStateException("Expecting a MeteredSessionStore");
    } else {
      final StateSerdes<GenericKey, GenericRow> serdes = getSerdes(sessionStore);
      final Bytes rawKey = Bytes.wrap(serdes.rawKey(key));
      final SessionStore<Bytes, byte[]> wrapped = getInnermostStore(sessionStore);
      final KeyValueIterator<Windowed<Bytes>, byte[]> fetch = wrapped.fetch(rawKey);
      return new DeserializingIterator(fetch, serdes);
    }
  }

  /*
  This method is used for range queries. It calls the fetchRangeUncached method.
   */
  public static KeyValueIterator<Windowed<GenericKey>, GenericRow> fetchRange(
          final ReadOnlySessionStore<GenericKey, GenericRow> store,
          final GenericKey keyFrom,
          final GenericKey keyTo
  ) {
    Objects.requireNonNull(keyFrom, "lower key can't be null");
    Objects.requireNonNull(keyTo, "upper key can't be null");

    final List<ReadOnlySessionStore<GenericKey, GenericRow>> stores = getStores(store);
    final Function<ReadOnlySessionStore<GenericKey, GenericRow>,
            KeyValueIterator<Windowed<GenericKey>, GenericRow>> fetchFunc
            = sessionStore -> fetchRangeUncached(sessionStore, keyFrom, keyTo);
    return findFirstNonEmptyIterator(stores, fetchFunc);
  }

  /*
  This method is used for range queries. It is invoked by the fetchRange method
   */
  private static KeyValueIterator<Windowed<GenericKey>, GenericRow> fetchRangeUncached(
          final ReadOnlySessionStore<GenericKey, GenericRow> sessionStore,
          final GenericKey keyFrom,
          final GenericKey keyTo
  ) {
    if (!(sessionStore instanceof MeteredSessionStore)) {
      throw new IllegalStateException("Expecting a MeteredSessionStore");
    } else {

      final StateSerdes<GenericKey, GenericRow> serdes = getSerdes(sessionStore);
      final Bytes rawKeyFrom = Bytes.wrap(serdes.rawKey(keyFrom));
      final Bytes rawKeyTo = Bytes.wrap(serdes.rawKey(keyTo));
      final SessionStore<Bytes, byte[]> wrapped = getInnermostStore(sessionStore);
      final KeyValueIterator<Windowed<Bytes>, byte[]> fetch = wrapped.fetch(rawKeyFrom, rawKeyTo);
      return new DeserializingIterator(fetch, serdes);
    }
  }

  private static KeyValueIterator<Windowed<GenericKey>, GenericRow> findFirstNonEmptyIterator(
          final List<ReadOnlySessionStore<GenericKey, GenericRow>> stores,
          final Function<ReadOnlySessionStore<GenericKey, GenericRow>,
                  KeyValueIterator<Windowed<GenericKey>, GenericRow>> fetchFunc
  ) {
    for (final ReadOnlySessionStore<GenericKey, GenericRow> sessionStore : stores) {
      try {
        final KeyValueIterator<Windowed<GenericKey>, GenericRow> result
                = fetchFunc.apply(sessionStore);
        // returns the first non-empty iterator
        if (!result.hasNext()) {
          result.close();
        } else {
          return result;
        }
      } catch (final InvalidStateStoreException e) {
        throw new InvalidStateStoreException(STORE_UNAVAILABLE_MESSAGE, e);
      }
    }
    return new EmptyKeyValueIterator();
  }

  @SuppressWarnings("unchecked")
  private static StateSerdes<GenericKey, GenericRow> getSerdes(
          final ReadOnlySessionStore<GenericKey, GenericRow> sessionStore
  ) throws RuntimeException {
    try {
      return (StateSerdes<GenericKey, GenericRow>) SERDES_FIELD.get(sessionStore);
    } catch (final IllegalAccessException e) {
      throw new RuntimeException("Stream internals changed unexpectedly!", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static SessionStore<Bytes, byte[]> getInnermostStore(
          final ReadOnlySessionStore<GenericKey, GenericRow> sessionStore
  ) {
    SessionStore<Bytes, byte[]> wrapped
            = ((MeteredSessionStore<GenericKey, GenericRow>) sessionStore).wrapped();
    // Unwrap state stores until we get to the last SessionStore, which is past the caching
    // layer.
    while (wrapped instanceof WrappedStateStore) {
      final StateStore store = ((WrappedStateStore<?, ?, ?>) wrapped).wrapped();
      // A RocksDBSessionStore wraps a SegmentedBytesStore, which isn't a SessionStore, so
      // we just store there.
      if (!(store instanceof SessionStore)) {
        break;
      }
      wrapped = (SessionStore<Bytes, byte[]>) store;
    }
    // now we have the innermost layer of the store.
    return wrapped;
  }

  @SuppressWarnings("unchecked")
  private static List<ReadOnlySessionStore<GenericKey, GenericRow>> getStores(
          final ReadOnlySessionStore<GenericKey, GenericRow> store
  ) {
    final QueryableStoreType<ReadOnlySessionStore<GenericKey, GenericRow>> storeType;
    try {
      final StateStoreProvider provider = (StateStoreProvider) PROVIDER_FIELD.get(store);
      final String storeName = (String) STORE_NAME_FIELD.get(store);
      storeType = (QueryableStoreType<ReadOnlySessionStore<GenericKey, GenericRow>>)
              STORE_TYPE_FIELD.get(store);
      return provider.stores(storeName, storeType);
    } catch (final IllegalAccessException e) {
      throw new RuntimeException("Stream internals changed unexpectedly!", e);
    }
  }

  private static final class DeserializingIterator
      implements KeyValueIterator<Windowed<GenericKey>, GenericRow> {
    private final KeyValueIterator<Windowed<Bytes>, byte[]> fetch;
    private final StateSerdes<GenericKey, GenericRow> serdes;

    private DeserializingIterator(final KeyValueIterator<Windowed<Bytes>, byte[]> fetch,
        final StateSerdes<GenericKey, GenericRow> serdes) {
      this.fetch = fetch;
      this.serdes = serdes;
    }

    @Override
    public void close() {
      fetch.close();
    }

    @Override
    public Windowed<GenericKey> peekNextKey() {
      final Windowed<Bytes> windowed = fetch.peekNextKey();
      return new Windowed<>(serdes.keyFrom(windowed.key().get()), windowed.window());
    }

    @Override
    public boolean hasNext() {
      return fetch.hasNext();
    }

    @Override
    public KeyValue<Windowed<GenericKey>, GenericRow> next() {
      final Windowed<GenericKey> key = peekNextKey();
      final KeyValue<Windowed<Bytes>, byte[]> next = fetch.next();
      return KeyValue.pair(key, serdes.valueFrom(next.value));
    }
  }

  private static class EmptyKeyValueIterator
      implements KeyValueIterator<Windowed<GenericKey>, GenericRow> {

    @Override
    public void close() {
    }

    @Override
    public Windowed<GenericKey> peekNextKey() {
      throw new NoSuchElementException();
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public KeyValue<Windowed<GenericKey>, GenericRow> next() {
      throw new NoSuchElementException();
    }
  }
}
