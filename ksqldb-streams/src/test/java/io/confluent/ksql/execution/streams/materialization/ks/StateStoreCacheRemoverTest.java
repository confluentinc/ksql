package io.confluent.ksql.execution.streams.materialization.ks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import java.time.Instant;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StateStoreCacheRemoverTest {

  private static final Schema SCHEMA = SchemaBuilder.struct().field("a", SchemaBuilder.int32());
  private static final Struct SOME_KEY = new Struct(SCHEMA).put("a", 1);

  @Mock
  private QueryableStoreType<ReadOnlyWindowStore<Struct, ValueAndTimestamp<GenericRow>>>
      queryableStoreType;
  @Mock
  private StateStoreProvider provider;
  @Mock
  private TimestampedWindowStore<Struct, ValueAndTimestamp<GenericRow>> windowStore;
  @Mock
  private WindowStoreIterator windowStoreIterator;

  private CompositeReadOnlyWindowStore<Struct, ValueAndTimestamp<GenericRow>> store;
  private WrappedStateStore<StateStore, Struct, ValueAndTimestamp<GenericRow>> wrappedStateStore;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    store = new CompositeReadOnlyWindowStore<>(provider, queryableStoreType, "foo");
    wrappedStateStore = mock(WrappedStateStore.class,
        withSettings().extraInterfaces(WindowStore.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldJustReturnStore() {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(windowStore));
    when(windowStore.fetch(any(), anyLong(), anyLong())).thenReturn(windowStoreIterator);
    when(windowStoreIterator.hasNext()).thenReturn(false);
    StateStoreCacheRemover.remove(store);
    store.fetch(SOME_KEY, Instant.ofEpochMilli(100), Instant.ofEpochMilli(200));
    verify(windowStore).fetch(SOME_KEY, 100L, 200L);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRemoveCache() {
    WrappedStateStore cacheStore
        = (WrappedStateStore) mock(StateStoreCacheRemover.CACHING_WINDOW_STORE_CLASS);
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(cacheStore));
    when(cacheStore.wrapped()).thenReturn(windowStore);
    when(windowStore.fetch(any(), anyLong(), anyLong())).thenReturn(windowStoreIterator);
    when(windowStoreIterator.hasNext()).thenReturn(false);

    StateStoreCacheRemover.remove(store);
    store.fetch(SOME_KEY, Instant.ofEpochMilli(100), Instant.ofEpochMilli(200));

    verify(windowStore).fetch(SOME_KEY, 100L, 200L);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRemoveCacheOneDeep() {
    WrappedStateStore cacheStore
        = (WrappedStateStore) mock(StateStoreCacheRemover.CACHING_WINDOW_STORE_CLASS);
    when(wrappedStateStore.wrapped())
        .thenReturn(cacheStore)
        // This allows us to read the newly set statestore
        .thenCallRealMethod();
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(wrappedStateStore));
    when(cacheStore.wrapped()).thenReturn(windowStore);
    when(((WindowStore)wrappedStateStore).fetch(any(), anyLong(), anyLong()))
        .thenReturn(windowStoreIterator);
    when(windowStoreIterator.hasNext()).thenReturn(false);

    StateStoreCacheRemover.remove(store);
    store.fetch(SOME_KEY, Instant.ofEpochMilli(100), Instant.ofEpochMilli(200));

    verify((WindowStore)wrappedStateStore).fetch(SOME_KEY, 100L, 200L);
    StateStore store = wrappedStateStore.wrapped();
    assertThat(store, is(windowStore));
  }
}
