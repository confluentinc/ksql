package io.confluent.ksql.execution.streams.materialization.ks;

import io.confluent.ksql.GenericRow;
import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.CompositeReadOnlySessionStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

public class StateStoreCacheRemover {
  private static final String CACHING_WINDOW_STORE_CLASS_NAME =
      "org.apache.kafka.streams.state.internals.CachingWindowStore";
  private static final String CACHING_SESSION_STORE_CLASS_NAME =
      "org.apache.kafka.streams.state.internals.CachingSessionStore";
  static final Class CACHING_WINDOW_STORE_CLASS;
  static final Class CACHING_SESSION_STORE_CLASS;

  static {
    try {
      CACHING_WINDOW_STORE_CLASS = Class.forName(CACHING_WINDOW_STORE_CLASS_NAME);
      CACHING_SESSION_STORE_CLASS = Class.forName(CACHING_SESSION_STORE_CLASS_NAME);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Can't find " + CACHING_WINDOW_STORE_CLASS_NAME);
    }
  }

  public static void remove(
      ReadOnlyWindowStore<Struct, ValueAndTimestamp<GenericRow>> store
  ) {
    if (!(store instanceof CompositeReadOnlyWindowStore)) {
      throw new RuntimeException("Streams internals changed unexpectedly!");
    }
    replaceProvider(CompositeReadOnlyWindowStore.class, store, "provider");
  }

  public static void remove(
      ReadOnlySessionStore<Struct, GenericRow> store
  ) {
    if (!(store instanceof CompositeReadOnlySessionStore)) {
      throw new RuntimeException("Streams internals changed unexpectedly!");
    }
    replaceProvider(CompositeReadOnlySessionStore.class, store, "storeProvider");
  }

  private static <T> void replaceProvider(
      Class<T> clazz,
      Object composite,
      String providerFieldName) {
    try {
      Field providerField = clazz.getDeclaredField(providerFieldName);
      providerField.setAccessible(true);
      StateStoreProvider old = (StateStoreProvider) providerField.get(composite);
      providerField.set(composite, new StoreProvider(old));
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Streams internals changed unexpectedly!");
    }
  }

  private static class StoreProvider implements StateStoreProvider {

    private final StateStoreProvider delegate;

    public StoreProvider(StateStoreProvider delegate) {
      this.delegate = delegate;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> stores(String s, QueryableStoreType<T> queryableStoreType) {
      List<T> storeList = delegate.stores(s, queryableStoreType);
      return storeList.stream()
          .map(store -> (T) StoreProvider.bypassCache((StateStore) store))
          .collect(Collectors.toList());
    }

    private static StateStore bypassCache(final StateStore store) {

      if (CACHING_WINDOW_STORE_CLASS.isInstance(store)
          || CACHING_SESSION_STORE_CLASS.isInstance(store)) {
        WrappedStateStore wrapped = (WrappedStateStore) store;
        return wrapped.wrapped();
      } else if (store instanceof WrappedStateStore) {
        WrappedStateStore wrapped = (WrappedStateStore) store;
        return swapOutWrapped(wrapped);
      } else {
        return store;
      }
    }

    private static WrappedStateStore swapOutWrapped(WrappedStateStore store) {
      final StateStore unwrapped = bypassCache(store.wrapped());
      try {
        Field wrappedField = WrappedStateStore.class.getDeclaredField("wrapped");
        wrappedField.setAccessible(true);
        wrappedField.set(store, unwrapped);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException("Streams internals changed unexpectedly!");
      }
      return store;
    }
  }
}
