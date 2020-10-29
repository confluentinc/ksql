package io.confluent.ksql.execution.streams.materialization.ks;

import static io.confluent.ksql.execution.streams.materialization.ks.SessionStoreCacheBypass.SERDES_FIELD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.CompositeReadOnlySessionStore;
import org.apache.kafka.streams.state.internals.MeteredSessionStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SessionStoreCacheBypassTest {

  private static final Schema SCHEMA = SchemaBuilder.struct().field("a", SchemaBuilder.int32());
  private static final Struct SOME_KEY = new Struct(SCHEMA).put("a", 1);
  private static final byte[] BYTES = new byte[] {'a', 'b'};

  @Mock
  private QueryableStoreType<ReadOnlySessionStore<Struct, GenericRow>>
      queryableStoreType;
  @Mock
  private StateStoreProvider provider;
  @Mock
  private MeteredSessionStore<Struct, GenericRow> meteredSessionStore;
  @Mock
  private SessionStore<Bytes, byte[]> sessionStore;
  @Mock
  private KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator;
  @Mock
  private StateSerdes<Struct, ValueAndTimestamp<GenericRow>> serdes;

  private CompositeReadOnlySessionStore<Struct, GenericRow> store;

  @Before
  public void setUp() {
    store = new CompositeReadOnlySessionStore<>(provider, queryableStoreType, "foo");
  }

  @Test
  public void shouldCallUnderlyingStore() throws IllegalAccessException {
    when(provider.stores(any(), any())).thenReturn(ImmutableList.of(meteredSessionStore));
    SERDES_FIELD.set(meteredSessionStore, serdes);
    when(serdes.rawKey(any())).thenReturn(BYTES);
    when(meteredSessionStore.wrapped()).thenReturn(sessionStore);
    when(sessionStore.fetch(any())).thenReturn(storeIterator);
    when(storeIterator.hasNext()).thenReturn(false);

    SessionStoreCacheBypass.fetch(store, SOME_KEY);
    verify(sessionStore).fetch(new Bytes(BYTES));
  }

  @Test
  public void shouldThrowException() throws IllegalAccessException {
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
}
