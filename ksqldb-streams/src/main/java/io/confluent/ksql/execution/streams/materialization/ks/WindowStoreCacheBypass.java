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
import java.io.Closeable;
import java.lang.reflect.Field;
import java.time.Instant;
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
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.MeteredWindowStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

public final class WindowStoreCacheBypass {
  private static final Field PROVIDER_FIELD;
  private static final Field STORE_NAME_FIELD;
  private static final Field WINDOW_STORE_TYPE_FIELD;
  static final Field SERDES_FIELD;
  private static final String STORE_UNAVAILABLE_MESSAGE = "State store is not available anymore "
          + "and may have been migrated to another instance; "
          + "please re-discover its location from the state metadata.";

  static {
    try {
      PROVIDER_FIELD = CompositeReadOnlyWindowStore.class.getDeclaredField("provider");
      PROVIDER_FIELD.setAccessible(true);
      STORE_NAME_FIELD = CompositeReadOnlyWindowStore.class.getDeclaredField("storeName");
      STORE_NAME_FIELD.setAccessible(true);
      WINDOW_STORE_TYPE_FIELD
          = CompositeReadOnlyWindowStore.class.getDeclaredField("windowStoreType");
      WINDOW_STORE_TYPE_FIELD.setAccessible(true);
      SERDES_FIELD = MeteredWindowStore.class.getDeclaredField("serdes");
      SERDES_FIELD.setAccessible(true);
    } catch (final NoSuchFieldException e) {
      throw new RuntimeException("Stream internals changed unexpectedly!", e);
    }
  }

  private WindowStoreCacheBypass() {

  }

  interface WindowStoreCacheBypassFetcher {

    WindowStoreIterator<ValueAndTimestamp<GenericRow>> fetch(
        ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> store,
        GenericKey key,
        Instant lower,
        Instant upper
    );

  }

  interface WindowStoreCacheBypassFetcherAll {

    KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>> fetchAll(
            ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> store,
            Instant lower,
            Instant upper
    );

  }

  interface WindowStoreCacheBypassFetcherRange {

    KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>> fetchRange(
            ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> store,
            GenericKey keyFrom,
            GenericKey keyTo,
            Instant lower,
            Instant upper
    );

  }

  /*
  This method is used for single key lookups. It calls the fetchUncached method.
   */
  public static WindowStoreIterator<ValueAndTimestamp<GenericRow>> fetch(
      final ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> store,
      final GenericKey key,
      final Instant lower,
      final Instant upper
  ) {
    Objects.requireNonNull(key, "key can't be null");
    final List<ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>>> stores
            = getStores(store);
    final Function<ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>>,
            WindowStoreIterator<ValueAndTimestamp<GenericRow>>> fetchFunc = windowStore ->
            fetchUncached(windowStore, key, lower, upper);
    return findFirstNonEmptyIterator(stores, fetchFunc);
  }

  /*
  This method is used for single key lookups. It is invoked by the fetch method
   */
  private static WindowStoreIterator<ValueAndTimestamp<GenericRow>> fetchUncached(
      final ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> windowStore,
      final GenericKey key,
      final Instant lower,
      final Instant upper
  ) {
    if (!(windowStore instanceof MeteredWindowStore)) {
      throw new IllegalStateException("Expecting a MeteredWindowStore");
    }
    final StateSerdes<GenericKey, ValueAndTimestamp<GenericRow>> serdes = getSerdes(windowStore);
    final WindowStore<Bytes, byte[]> wrapped = getInnermostStore(windowStore);
    final Bytes rawKey = Bytes.wrap(serdes.rawKey(key));
    final WindowStoreIterator<byte[]> fetch = wrapped.fetch(rawKey, lower, upper);
    return new DeserializingIterator(fetch, serdes);
  }

  /*
  This method is used for range queries. It calls the fetchRangeUncached method.
   */
  public static KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>> fetchRange(
          final ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> store,
          final GenericKey keyFrom,
          final GenericKey keyTo,
          final Instant lower,
          final Instant upper
  ) {
    Objects.requireNonNull(keyFrom, "lower key can't be null");
    Objects.requireNonNull(keyTo, "upper key can't be null");
    final List<ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>>> stores
            = getStores(store);
    final Function<ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>>,
            KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> fetchFunc
                = windowStore -> fetchRangeUncached(windowStore, keyFrom, keyTo, lower, upper);
    return findFirstNonEmptyIterator(stores, fetchFunc);
  }

  /*
  This method is used for range queries. It is invoked by the fetchRange method
   */
  private static KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>
      fetchRangeUncached(
          final ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> windowStore,
          final GenericKey keyFrom,
          final GenericKey keyTo,
          final Instant lower,
          final Instant upper
  ) {
    if (!(windowStore instanceof MeteredWindowStore)) {
      throw new IllegalStateException("Expecting a MeteredWindowStore");
    }
    final StateSerdes<GenericKey, ValueAndTimestamp<GenericRow>> serdes = getSerdes(windowStore);
    final WindowStore<Bytes, byte[]> wrapped = getInnermostStore(windowStore);
    final Bytes rawKeyFrom = Bytes.wrap(serdes.rawKey(keyFrom));
    final Bytes rawKeyTo = Bytes.wrap(serdes.rawKey(keyTo));
    final KeyValueIterator<Windowed<Bytes>, byte[]> fetch = wrapped
            .fetch(rawKeyFrom, rawKeyTo, lower, upper);
    return new DeserializingKeyValueIterator(fetch, serdes);
  }

  /*
  This method is used for table scans. It calls the fetchAllUncached method.
   */
  public static KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>> fetchAll(
          final ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> store,
          final Instant lower,
          final Instant upper
  ) {
    final List<ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>>> stores
            = getStores(store);
    final Function<ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>>,
            KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> fetchFunc =
                windowStore -> fetchAllUncached(windowStore, lower, upper);
    return findFirstNonEmptyIterator(stores, fetchFunc);
  }

  /*
  This method is used for table scans. It is invoked by the fetchAll method.
   */
  private static KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>
      fetchAllUncached(
          final ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> windowStore,
          final Instant lower,
          final Instant upper
  ) {
    if (!(windowStore instanceof MeteredWindowStore)) {
      throw new IllegalStateException("Expecting a MeteredWindowStore");
    }
    final StateSerdes<GenericKey, ValueAndTimestamp<GenericRow>> serdes = getSerdes(windowStore);
    final WindowStore<Bytes, byte[]> wrapped = getInnermostStore(windowStore);
    final KeyValueIterator<Windowed<Bytes>, byte[]> fetch = wrapped.fetchAll(lower, upper);
    return new DeserializingKeyValueIterator(fetch, serdes);
  }

  @SuppressWarnings("unchecked")
  private static <T extends KeyValueIterator & Closeable> T findFirstNonEmptyIterator(
          final List<ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>>> stores,
          final Function<ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>>, T> func
  ) {
    T result = null;
    for (final ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> store : stores) {
      try {
        result = func.apply(store);
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
    return (T) (result instanceof WindowStoreIterator
            ? new EmptyWindowStoreIterator() : new EmptyKeyValueIterator());
  }

  @SuppressWarnings("unchecked")
  private static StateSerdes<GenericKey, ValueAndTimestamp<GenericRow>> getSerdes(
          final ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> windowStore
  ) throws RuntimeException {
    try {
      return (StateSerdes<GenericKey, ValueAndTimestamp<GenericRow>>) SERDES_FIELD.get(windowStore);
    } catch (final IllegalAccessException e) {
      throw new RuntimeException("Stream internals changed unexpectedly!", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static WindowStore<Bytes, byte[]> getInnermostStore(
          final ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> windowStore
  ) {
    WindowStore<Bytes, byte[]> wrapped
            = ((MeteredWindowStore<GenericKey, ValueAndTimestamp<GenericRow>>) windowStore)
            .wrapped();
    // Unwrap state stores until we get to the last WindowStore, which is past the caching
    // layer.
    while (wrapped instanceof WrappedStateStore) {
      final StateStore store = ((WrappedStateStore<?, ?, ?>) wrapped).wrapped();
      // A RocksDBWindowStore wraps a SegmentedBytesStore, which isn't a SessionStore, so
      // we just store there.
      if (!(store instanceof WindowStore)) {
        break;
      }
      wrapped = (WindowStore<Bytes, byte[]>) store;
    }
    // now we have the innermost layer of the store.
    return wrapped;
  }

  @SuppressWarnings("unchecked")
  private static List<ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>>> getStores(
          final ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>> store
  ) {
    try {
      final StateStoreProvider provider = (StateStoreProvider) PROVIDER_FIELD.get(store);
      final String storeName = (String) STORE_NAME_FIELD.get(store);
      final QueryableStoreType<ReadOnlyWindowStore<GenericKey, ValueAndTimestamp<GenericRow>>>
              windowStoreType = (QueryableStoreType<ReadOnlyWindowStore<GenericKey,
              ValueAndTimestamp<GenericRow>>>) WINDOW_STORE_TYPE_FIELD.get(store);
      return provider.stores(storeName, windowStoreType);
    } catch (final IllegalAccessException e) {
      throw new RuntimeException("Stream internals changed unexpectedly!", e);
    }
  }

  /*
  This iterator is used for range queries/table scans in the fetchRangeUncached
  and fetchAllUncached methods
   */
  private static final class DeserializingKeyValueIterator
          implements KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>> {
    private final KeyValueIterator<Windowed<Bytes>, byte[]> fetch;
    private final StateSerdes<GenericKey, ValueAndTimestamp<GenericRow>> serdes;

    private DeserializingKeyValueIterator(
            final KeyValueIterator<Windowed<Bytes>, byte[]> fetch,
            final StateSerdes<GenericKey, ValueAndTimestamp<GenericRow>> serdes) {
      this.fetch = fetch;
      this.serdes = serdes;
    }

    @Override
    public void close() {
      fetch.close();
    }

    @Override
    public Windowed<GenericKey> peekNextKey() {
      final Windowed<Bytes> nextKey = fetch.peekNextKey();
      return new Windowed<>(serdes.keyFrom(nextKey.key().get()), nextKey.window());
    }

    @Override
    public boolean hasNext() {
      return fetch.hasNext();
    }

    @Override
    public KeyValue<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>> next() {
      final KeyValue<Windowed<Bytes>, byte[]> next = fetch.next();
      final Windowed<GenericKey> windowedKey = new Windowed<>(
              serdes.keyFrom(next.key.key().get()), next.key.window());
      return KeyValue.pair(windowedKey, serdes.valueFrom(next.value));
    }
  }

  /*
  This iterator is used for key lookups in the fetchUncached method
   */
  private static final class DeserializingIterator
          implements WindowStoreIterator<ValueAndTimestamp<GenericRow>> {
    private final WindowStoreIterator<byte[]> fetch;
    private final StateSerdes<GenericKey, ValueAndTimestamp<GenericRow>> serdes;

    private DeserializingIterator(
            final WindowStoreIterator<byte[]> fetch,
            final StateSerdes<GenericKey, ValueAndTimestamp<GenericRow>> serdes) {
      this.fetch = fetch;
      this.serdes = serdes;
    }

    @Override
    public void close() {
      fetch.close();
    }

    @Override
    public Long peekNextKey() {
      return fetch.peekNextKey();
    }

    @Override
    public boolean hasNext() {
      return fetch.hasNext();
    }

    @Override
    public KeyValue<Long, ValueAndTimestamp<GenericRow>> next() {
      final KeyValue<Long, byte[]> next = fetch.next();
      return KeyValue.pair(next.key, serdes.valueFrom(next.value));
    }
  }

  private static class EmptyWindowStoreIterator
          implements WindowStoreIterator<ValueAndTimestamp<GenericRow>> {

    @Override
    public void close() {
    }

    @Override
    public Long peekNextKey() {
      throw new NoSuchElementException();
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public KeyValue<Long, ValueAndTimestamp<GenericRow>> next() {
      throw new NoSuchElementException();
    }
  }

  private static class EmptyKeyValueIterator
          implements KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>> {

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
    public KeyValue<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>> next() {
      throw new NoSuchElementException();
    }
  }
}
