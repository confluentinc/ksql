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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import java.time.Instant;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
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

  private static final Schema SCHEMA = SchemaBuilder.struct().field("a", SchemaBuilder.int32());
  private static final Struct SOME_KEY = new Struct(SCHEMA).put("a", 1);
  private static final byte[] BYTES = new byte[] {'a', 'b'};

  @Mock
  private QueryableStoreType<ReadOnlyWindowStore<Struct, ValueAndTimestamp<GenericRow>>>
      queryableStoreType;
  @Mock
  private StateStoreProvider provider;
  @Mock
  private MeteredWindowStore<Struct, ValueAndTimestamp<GenericRow>> meteredWindowStore;
  @Mock
  private WindowStore<Bytes, byte[]> windowStore;
  @Mock
  private WindowStoreIterator<byte[]> windowStoreIterator;
  @Mock
  private StateSerdes<Struct, ValueAndTimestamp<GenericRow>> serdes;

  private CompositeReadOnlyWindowStore<Struct, ValueAndTimestamp<GenericRow>> store;
  private WrappedStateStore<StateStore, Struct, ValueAndTimestamp<GenericRow>> wrappedStateStore;

  @Before
  public void setUp() {
    store = new CompositeReadOnlyWindowStore<>(provider, queryableStoreType, "foo");
  }

  @Test
  public void shouldCallUnderlyingStore() throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredWindowStore));
    SERDES_FIELD.set(meteredWindowStore, serdes);
    when(serdes.rawKey(any())).thenReturn(BYTES);
    when(meteredWindowStore.wrapped()).thenReturn(windowStore);
    when(windowStore.fetch(any(), any(), any())).thenReturn(windowStoreIterator);
    when(windowStoreIterator.hasNext()).thenReturn(false);

    WindowStoreCacheBypass.fetch(store, SOME_KEY, Instant.ofEpochMilli(100), Instant.ofEpochMilli(200));
    verify(windowStore).fetch(new Bytes(BYTES), Instant.ofEpochMilli(100L), Instant.ofEpochMilli(200L));
  }

  @Test
  public void shouldThrowException() throws IllegalAccessException {
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
}
