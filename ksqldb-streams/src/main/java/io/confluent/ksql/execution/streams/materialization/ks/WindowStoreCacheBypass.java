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
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
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

public final class WindowStoreCacheBypass {
  private static final Field PROVIDER_FIELD;
  private static final Field STORE_NAME_FIELD;
  private static final Field WINDOW_STORE_TYPE_FIELD;
  static final Field SERDES_FIELD;

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

  private WindowStoreCacheBypass() {}

  interface WindowStoreCacheBypassFetcher {

    WindowStoreIterator<ValueAndTimestamp<GenericRow>> fetch(
        ReadOnlyWindowStore<Struct, ValueAndTimestamp<GenericRow>> store,
        Struct key,
        Instant lower,
        Instant upper
    );
  }

  @SuppressWarnings("unchecked")
  public static WindowStoreIterator<ValueAndTimestamp<GenericRow>> fetch(
      final ReadOnlyWindowStore<Struct, ValueAndTimestamp<GenericRow>> store,
      final Struct key,
      final Instant lower,
      final Instant upper
  ) {
    Objects.requireNonNull(key, "key can't be null");

    final StateStoreProvider provider;
    final String storeName;
    final QueryableStoreType<ReadOnlyWindowStore<Struct, ValueAndTimestamp<GenericRow>>>
        windowStoreType;
    try {
      provider = (StateStoreProvider) PROVIDER_FIELD.get(store);
      storeName = (String) STORE_NAME_FIELD.get(store);
      windowStoreType = (QueryableStoreType<ReadOnlyWindowStore<Struct,
          ValueAndTimestamp<GenericRow>>>) WINDOW_STORE_TYPE_FIELD.get(store);
    } catch (final IllegalAccessException e) {
      throw new RuntimeException("Stream internals changed unexpectedly!", e);
    }
    final List<ReadOnlyWindowStore<Struct, ValueAndTimestamp<GenericRow>>> stores
        = provider.stores(storeName, windowStoreType);
    for (final ReadOnlyWindowStore<Struct, ValueAndTimestamp<GenericRow>> windowStore : stores) {
      try {
        final WindowStoreIterator<ValueAndTimestamp<GenericRow>> result
            = fetchUncached(windowStore, key, lower, upper);
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
  private static WindowStoreIterator<ValueAndTimestamp<GenericRow>> fetchUncached(
      final ReadOnlyWindowStore<Struct, ValueAndTimestamp<GenericRow>> windowStore,
      final Struct key,
      final Instant lower,
      final Instant upper
  ) {
    if (windowStore instanceof MeteredWindowStore) {
      final StateSerdes<Struct, ValueAndTimestamp<GenericRow>> serdes;
      try {
        serdes = (StateSerdes<Struct, ValueAndTimestamp<GenericRow>>) SERDES_FIELD.get(windowStore);
      } catch (final IllegalAccessException e) {
        throw new RuntimeException("Stream internals changed unexpectedly!", e);
      }

      final Bytes rawKey = Bytes.wrap(serdes.rawKey(key));
      WindowStore<Bytes, byte[]> wrapped
          = ((MeteredWindowStore<Struct, ValueAndTimestamp<GenericRow>>) windowStore).wrapped();
      while (wrapped instanceof WrappedStateStore) {
        final StateStore store = ((WrappedStateStore<?, ?, ?>) wrapped).wrapped();
        if (!(store instanceof WindowStore)) {
          break;
        }
        wrapped = (WindowStore<Bytes, byte[]>) store;
      }
      // now we have the innermost layer of the store.
      final WindowStoreIterator<byte[]> fetch = wrapped.fetch(rawKey, lower, upper);
      return new DeserializingIterator(fetch, serdes);
    } else {
      throw new IllegalStateException("Expecting a MeteredWindowStore");
    }
  }

  private static final class DeserializingIterator
      implements WindowStoreIterator<ValueAndTimestamp<GenericRow>> {
    private final WindowStoreIterator<byte[]> fetch;
    private final StateSerdes<Struct, ValueAndTimestamp<GenericRow>> serdes;

    private DeserializingIterator(final WindowStoreIterator<byte[]> fetch,
        final StateSerdes<Struct, ValueAndTimestamp<GenericRow>> serdes) {
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

  private static class EmptyKeyValueIterator
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
}