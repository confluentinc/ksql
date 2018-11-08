/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Type-safe collection of handlers for different types.
 *
 * <p>Can be used to build a static map of handlers for certain types, e.g.
 * <pre>
 * {@code
 * class SomeClass {
 *   private static final HandlerMap1<SomeBaseType, SomeClass> HANDLERS =
 *       HandlerMaps.<SomeBaseType, SomeClass>builder1()
 *           .put(SomeDerivedType.class, SomeClass::someMethod)
 *           .put(SomeOtherDerivedType.class, SomeClass::someOtherMethod)
 *           .put0(AnotherDerivedType.class, SomeHandler::new)
 *           .build();
 *
 *   private void someMethod(final SomeDerivedType arg) {
 *     ...
 *   }
 *
 *   private static void someOtherMethod(final SomeOtherDerivedType arg) {
 *     ...
 *   }
 *
 *   private static final SomeHandler implements Handler0<AnotherDerivedType> {
 *   }
 * }
 * }
 * </pre>
 */
public final class HandlerMaps {

  private HandlerMaps() {
  }

  /**
   * Get a builder for handler map with no arguments.
   *
   * @param <K> The key of the map
   * @return the builder.
   */
  public static <K> Builder0<K> builder0() {
    return new Builder0<>();
  }

  /**
   * Get a builder for handler map with one argument.
   *
   * @param <K> The key of the map
   * @param <A0> The type of the first argument.
   * @return the builder.
   */
  public static <K, A0> Builder1<K, A0> builder1() {
    return new Builder1<>();
  }

  /**
   * Get a builder for handler map with two argument.
   *
   * @param <K> The key of the map
   * @param <A0> The type of the first argument.
   * @param <A1> The type of the second argument.
   * @return the builder.
   */
  public static <K, A0, A1> Builder2<K, A0, A1> builder2() {
    return new Builder2<>();
  }

  @FunctionalInterface
  public interface Handler0<K> {

    void handle(K key);
  }

  @FunctionalInterface
  public interface Handler1<K, A0> {

    void handle(A0 arg0, K key);
  }

  @FunctionalInterface
  public interface Handler2<K, A0, A1> {

    void handle(A0 arg0, A1 arg1, K key);
  }

  public static class Builder0<K> {

    private final Map<Class<? extends K>, Handler0<K>> handlers = Maps.newHashMap();

    /**
     * Add a new no-arg handler to the map.
     *
     * <p>Allows the passing of handlers via lambdas.
     *
     * <p>e.g. put(Some.class, SomeHandler::new)
     *
     * <p>Where {@code SomeHandler} implements {@code Handler0}. This means {@code SomeHandler}
     * does not count towards the classes data abstraction coupling
     *
     * @param type the key type to register the handler against
     * @param supplier the supplier of the handler. The supplier will be called immediately.
     * @param <KT> the derived key type.
     * @return set.
     */
    public <KT extends K> Builder0<K> put(
        final Class<KT> type,
        final Supplier<? extends Handler0<? super KT>> supplier) {

      final Handler0<? super KT> handler;

      try {
        handler = supplier.get();
      } catch (final Exception e) {
        throw new IllegalArgumentException("Failed to get handler for type: " + type, e);
      }

      if (handler == null) {
        throw new IllegalArgumentException("Null handler returned by supplier for type: " + type);
      }

      return put(type, handler);
    }

    /**
     * Add a new no-arg handler to the map.
     *
     * <p>This can be useful for calling static methods
     *
     * @param type the key type to register the handler against
     * @param handler the handler to the supplied {@code type}
     * @param <KT> the derived type the handler handles
     * @return set.
     */
    public <KT extends K> Builder0<K> put(
        final Class<KT> type,
        final Handler0<? super KT> handler) {

      if (handlers.containsKey(type)) {
        throw new IllegalArgumentException("Duplicate key: " + type);
      }
      handlers.put(type, castHandler0(type, handler));
      return this;
    }

    /**
     * @return the built handler map.
     */
    public HandlerMap0<K> build() {
      return new HandlerMap0<>(handlers);
    }

    private <KT extends K> Handler0<K> castHandler0(
        final Class<KT> keyType,
        final Handler0<? super KT> handler) {

      return k -> handler.handle(keyType.cast(k));
    }
  }

  public static final class HandlerMap0<K> {

    private final Map<Class<? extends K>, Handler0<K>> handlers;

    private HandlerMap0(final Map<Class<? extends K>, Handler0<K>> handlers) {
      this.handlers = ImmutableMap.copyOf(handlers);
    }

    public Handler0<K> get(final Class<? extends K> type) {
      return handlers.get(type);
    }

    @SuppressWarnings("unchecked")
    public <KT extends K> Handler0<KT> getTyped(final Class<KT> type) {
      return (Handler0<KT>) handlers.get(type);
    }

    @SuppressWarnings("unchecked")
    public Handler0<K> getOrDefault(
        final Class<? extends K> type,
        final Handler0<? super K> defaultHandler) {
      return handlers.getOrDefault(type, (Handler0<K>) defaultHandler);
    }
  }

  public static class Builder1<K, A0> {

    private final Map<Class<? extends K>, Handler1<K, A0>> handlers = Maps.newHashMap();

    /**
     * Add a new no-arg handler to the map.
     *
     * <p>Allows the passing of handlers via lambdas.
     *
     * <p>e.g. put(Some.class, SomeHandler::new)
     *
     * <p>Where {@code SomeHandler} implements {@code Handler0}. This means {@code SomeHandler}
     * does not count towards the classes data abstraction coupling
     *
     * @param type the key type to register the handler against
     * @param supplier the supplier of the handler. The supplier will be called immediately.
     * @param <KT> the derived key type.
     * @return set.
     */
    public <KT extends K> Builder1<K, A0> put0(
        final Class<KT> type,
        final Supplier<? extends Handler0<? super KT>> supplier) {

      final Handler0<? super KT> handler;

      try {
        handler = supplier.get();
      } catch (final Exception e) {
        throw new IllegalArgumentException("Failed to get handler for type: " + type, e);
      }

      if (handler == null) {
        throw new IllegalArgumentException("Null handler returned by supplier for type: " + type);
      }

      return put(type, handler);
    }

    /**
     * Add a new no-arg handler to the map.
     *
     * <p>This can be useful for calling static methods
     *
     * @param type the key type to register the handler against
     * @param handler the handler to the supplied {@code type}
     * @param <KT> the derived type the handler handles
     * @return set.
     */
    public <KT extends K> Builder1<K, A0> put(
        final Class<KT> type,
        final Handler0<? super KT> handler) {

      if (handlers.containsKey(type)) {
        throw new IllegalArgumentException("Duplicate key: " + type);
      }
      handlers.put(type, castHandler0(type, handler));
      return this;
    }

    /**
     * Add a new single-arg handler to the map.
     *
     * @param type the key type to register the handler against
     * @param handler the handler to the supplied {@code type}
     * @param <KT> the derived type the handler handles
     * @return set.
     */
    public <KT extends K> Builder1<K, A0> put(
        final Class<KT> type,
        final Handler1<? super KT, ? super A0> handler) {

      if (handlers.containsKey(type)) {
        throw new IllegalArgumentException("Duplicate key: " + type);
      }
      handlers.put(type, castHandler1(type, handler));
      return this;
    }

    /**
     * Add a new single-arg handler to the map.
     *
     * <p>Allows the passing of handlers via lambdas.
     *
     * <p>e.g. put(Some.class, SomeHandler::new)
     *
     * <p>Where {@code SomeHandler} implements {@code Handler1}. This means {@code SomeHandler}
     * does not count towards the classes data abstraction coupling
     *
     * @param type the key type to register the handler against
     * @param supplier the supplier of the handler. The supplier will be called immediately.
     * @param <KT> the derived key type.
     * @return set.
     */
    public <KT extends K> Builder1<K, A0> put(
        final Class<KT> type,
        final Supplier<? extends Handler1<? super KT, ? super A0>> supplier) {

      final Handler1<? super KT, ? super A0> handler;

      try {
        handler = supplier.get();
      } catch (final Exception e) {
        throw new IllegalArgumentException("Failed to get handler for type: " + type, e);
      }

      if (handler == null) {
        throw new IllegalArgumentException("Null handler returned by supplier for type: " + type);
      }

      return put(type, handler);
    }

    /**
     * @return the built handler map.
     */
    public HandlerMap1<K, A0> build() {
      return new HandlerMap1<>(handlers);
    }

    private <KT extends K> Handler1<K, A0> castHandler0(
        final Class<KT> keyType,
        final Handler0<? super KT> handler) {

      return (a0, k) -> handler.handle(keyType.cast(k));
    }

    private <KT extends K> Handler1<K, A0> castHandler1(
        final Class<KT> keyType,
        final Handler1<? super KT, ? super A0> handler) {

      return (a0, k) -> handler.handle(a0, keyType.cast(k));
    }
  }

  public static final class HandlerMap1<K, A0> {

    private final Map<Class<? extends K>, Handler1<K, A0>> handlers;

    private HandlerMap1(final Map<Class<? extends K>, Handler1<K, A0>> handlers) {
      this.handlers = ImmutableMap.copyOf(handlers);
    }

    public Handler1<K, A0> get(final Class<? extends K> type) {
      return handlers.get(type);
    }

    @SuppressWarnings("unchecked")
    public <KT extends K> Handler1<KT, A0> getTyped(final Class<KT> type) {
      return (Handler1<KT, A0>) handlers.get(type);
    }

    @SuppressWarnings("unchecked")
    public Handler1<K, A0> getOrDefault(
        final Class<? extends K> type,
        final Handler1<? super K, ? super A0> defaultHandler) {
      return handlers.getOrDefault(type, (Handler1<K, A0>) defaultHandler);
    }
  }

  public static class Builder2<K, A0, A1> {

    private final Map<Class<? extends K>, Handler2<K, A0, A1>> handlers = Maps.newHashMap();

    /**
     * Add a new no-arg handler to the map.
     *
     * <p>Allows the passing of handlers via lambdas.
     *
     * <p>e.g. put0(Some.class, SomeHandler::new)
     *
     * <p>Where {@code SomeHandler} implements {@code Handler0}. This means {@code SomeHandler}
     * does not count towards the classes data abstraction coupling
     *
     * @param type the key type to register the handler against
     * @param supplier the supplier of the handler. The supplier will be called immediately.
     * @param <KT> the derived key type.
     * @return set.
     */
    public <KT extends K> Builder2<K, A0, A1> put0(
        final Class<KT> type,
        final Supplier<? extends Handler0<? super KT>> supplier) {

      final Handler0<? super KT> handler;

      try {
        handler = supplier.get();
      } catch (final Exception e) {
        throw new IllegalArgumentException("Failed to get handler for type: " + type, e);
      }

      if (handler == null) {
        throw new IllegalArgumentException("Null handler returned by supplier for type: " + type);
      }

      return put(type, handler);
    }

    /**
     * Add a new one-arg handler to the map.
     *
     * <p>Allows the passing of handlers via lambdas.
     *
     * <p>e.g. put1(Some.class, SomeHandler::new)
     *
     * <p>Where {@code SomeHandler} implements {@code Handler1}. This means {@code SomeHandler}
     * does not count towards the classes data abstraction coupling
     *
     * @param type the key type to register the handler against
     * @param supplier the supplier of the handler. The supplier will be called immediately.
     * @param <KT> the derived key type.
     * @return set.
     */
    public <KT extends K> Builder2<K, A0, A1> put1(
        final Class<KT> type,
        final Supplier<? extends Handler1<? super KT, ? super A0>> supplier) {

      final Handler1<? super KT, ? super A0> handler;

      try {
        handler = supplier.get();
      } catch (final Exception e) {
        throw new IllegalArgumentException("Failed to get handler for type: " + type, e);
      }

      if (handler == null) {
        throw new IllegalArgumentException("Null handler returned by supplier for type: " + type);
      }

      return put(type, handler);
    }

    /**
     * Add a new two-arg handler to the map.
     *
     * <p>Allows the passing of handlers via lambdas.
     *
     * <p>e.g. put(Some.class, SomeHandler::new)
     *
     * <p>Where {@code SomeHandler} implements {@code Handler1}. This means {@code SomeHandler}
     * does not count towards the classes data abstraction coupling
     *
     * @param type the key type to register the handler against
     * @param supplier the supplier of the handler. The supplier will be called immediately.
     * @param <KT> the derived key type.
     * @return set.
     */
    public <KT extends K> Builder2<K, A0, A1> put(
        final Class<KT> type,
        final Supplier<? extends Handler2<? super KT, ? super A0, ? super A1>> supplier) {

      final Handler2<? super KT, ? super A0, ? super A1> handler;

      try {
        handler = supplier.get();
      } catch (final Exception e) {
        throw new IllegalArgumentException("Failed to get handler for type: " + type, e);
      }

      if (handler == null) {
        throw new IllegalArgumentException("Null handler returned by supplier for type: " + type);
      }

      return put(type, handler);
    }

    /**
     * Add a new no-arg handler to the map.
     *
     * <p>This can be useful for calling static methods
     *
     * @param type the key type to register the handler against
     * @param handler the handler to the supplied {@code type}
     * @param <KT> the derived type the handler handles
     * @return set.
     */
    public <KT extends K> Builder2<K, A0, A1> put(
        final Class<KT> type,
        final Handler0<? super KT> handler) {

      if (handlers.containsKey(type)) {
        throw new IllegalArgumentException("Duplicate key: " + type);
      }
      handlers.put(type, castHandler0(type, handler));
      return this;
    }

    /**
     * Add a new single-arg handler to the map.
     *
     * @param type the key type to register the handler against
     * @param handler the handler to the supplied {@code type}
     * @param <KT> the derived type the handler handles
     * @return set.
     */
    public <KT extends K> Builder2<K, A0, A1> put(
        final Class<KT> type,
        final Handler1<? super KT, ? super A0> handler) {

      if (handlers.containsKey(type)) {
        throw new IllegalArgumentException("Duplicate key: " + type);
      }
      handlers.put(type, castHandler1(type, handler));
      return this;
    }

    /**
     * Add a new two-arg handler to the map.
     *
     * @param type the key type to register the handler against
     * @param handler the handler to the supplied {@code type}
     * @param <KT> the derived type the handler handles
     * @return set.
     */
    public <KT extends K> Builder2<K, A0, A1> put(
        final Class<KT> type,
        final Handler2<? super KT, ? super A0, ? super A1> handler) {

      if (handlers.containsKey(type)) {
        throw new IllegalArgumentException("Duplicate key: " + type);
      }
      handlers.put(type, castHandler2(type, handler));
      return this;
    }

    /**
     * @return the built handler map.
     */
    public HandlerMap2<K, A0, A1> build() {
      return new HandlerMap2<>(handlers);
    }

    private <KT extends K> Handler2<K, A0, A1> castHandler0(
        final Class<KT> keyType,
        final Handler0<? super KT> handler) {

      return (a0, a1, k) -> handler.handle(keyType.cast(k));
    }

    private <KT extends K> Handler2<K, A0, A1> castHandler1(
        final Class<KT> keyType,
        final Handler1<? super KT, ? super A0> handler) {

      return (a0, a1, k) -> handler.handle(a0, keyType.cast(k));
    }

    private <KT extends K> Handler2<K, A0, A1> castHandler2(
        final Class<KT> keyType,
        final Handler2<? super KT, ? super A0, ? super A1> handler) {

      return (a0, a1, k) -> handler.handle(a0, a1, keyType.cast(k));
    }
  }

  public static final class HandlerMap2<K, A0, A1> {

    private final Map<Class<? extends K>, Handler2<K, A0, A1>> handlers;

    private HandlerMap2(final Map<Class<? extends K>, Handler2<K, A0, A1>> handlers) {
      this.handlers = ImmutableMap.copyOf(handlers);
    }

    public Handler2<K, A0, A1> get(final Class<? extends K> type) {
      return handlers.get(type);
    }

    @SuppressWarnings("unchecked")
    public <KT extends K> Handler2<KT, A0, A1> getTyped(final Class<KT> type) {
      return (Handler2<KT, A0, A1>) handlers.get(type);
    }

    @SuppressWarnings("unchecked")
    public Handler2<K, A0, A1> getOrDefault(
        final Class<? extends K> type,
        final Handler2<? super K, ? super A0, ? super A1> defaultHandler) {
      return handlers.getOrDefault(type, (Handler2<K, A0, A1>) defaultHandler);
    }
  }
}
