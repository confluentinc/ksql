/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Type-safe collection of handlers for different types.
 *
 * <p>Can be used to build a static map of handlers for certain types, e.g.
 * <pre>
 * {@code
 * class SomeClass {
 *   private static final ClassHandlerMap1<SomeBaseType, SomeClass> HANDLERS =
 *       HandlerMaps.forType(SomeBaseType.class).withArgType(SomeClass.class)
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
@SuppressWarnings("WeakerAccess")
public final class HandlerMaps {

  private HandlerMaps() {
  }

  /**
   * Get a builder for handler map with no arguments.
   *
   * @param <K> The key of the map
   * @return the builder.
   */
  @SuppressWarnings("unused") // Required for automatic type inference
  public static <K> Builder0<K> forClass(final Class<K> type) {
    return new Builder0<>();
  }

  @FunctionalInterface
  public interface Handler0<K> {

    void handle(K key);
  }

  @FunctionalInterface
  public interface HandlerR0<K, R> {

    R handle(K key);
  }

  @FunctionalInterface
  public interface Handler1<K, A0> {

    void handle(A0 arg0, K key);
  }

  @FunctionalInterface
  public interface HandlerR1<K, A0, R> {

    R handle(A0 arg0, K key);
  }

  @FunctionalInterface
  public interface Handler2<K, A0, A1> {

    void handle(A0 arg0, A1 arg1, K key);
  }

  @FunctionalInterface
  public interface HandlerR2<K, A0, A1, R> {

    R handle(A0 arg0, A1 arg1, K key);
  }

  public static class Builder0<K> {

    private final Map<Class<? extends K>, Handler0<K>> handlers = Maps.newHashMap();

    /**
     * Get a builder for this type with a single argument.
     *
     * <p>If {@code arg0} is a class, then member and static functions of the class can be used
     * as handler.
     *
     * @param <A0> The type of the first argument.
     * @return the builder.
     */
    @SuppressWarnings("unused") // Required for automatic type inference
    public <A0> Builder1<K, A0> withArgType(final Class<A0> arg0) {
      throwIfHandlers();
      return new Builder1<>();
    }

    /**
     * Get a builder for this type with a two argument.
     *
     * <p>If {@code arg0} is a class, then member and static functions of the class can be used
     * as handler.
     *
     * @param <A0> The type of the first argument.
     * @param <A1> The type of the second argument.
     * @return the builder.
     */
    @SuppressWarnings("unused")  // Required for automatic type inference
    public <A0, A1> Builder2<K, A0, A1> withArgTypes(
        final Class<A0> arg0,
        final Class<A1> arg1
    ) {
      throwIfHandlers();
      return new Builder2<>();
    }

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
    public ClassHandlerMap0<K> build() {
      return new ClassHandlerMap0<>(handlers);
    }

    private <KT extends K> Handler0<K> castHandler0(
        final Class<KT> keyType,
        final Handler0<? super KT> handler) {

      return k -> handler.handle(keyType.cast(k));
    }

    private void throwIfHandlers() {
      if (!handlers.isEmpty()) {
        throw new IllegalStateException("handlers defined");
      }
    }
  }

  public static final class ClassHandlerMap0<K> {

    private final Map<Class<? extends K>, Handler0<K>> handlers;

    private ClassHandlerMap0(final Map<Class<? extends K>, Handler0<K>> handlers) {
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

    public Set<Class<? extends K>> keySet() {
      return handlers.keySet();
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
    public ClassHandlerMap1<K, A0> build() {
      return new ClassHandlerMap1<>(handlers);
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

  public static final class ClassHandlerMap1<K, A0> {

    private final Map<Class<? extends K>, Handler1<K, A0>> handlers;

    private ClassHandlerMap1(final Map<Class<? extends K>, Handler1<K, A0>> handlers) {
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

    public Set<Class<? extends K>> keySet() {
      return handlers.keySet();
    }
  }

  public static class Builder2<K, A0, A1> {

    private final Map<Class<? extends K>, Handler2<K, A0, A1>> handlers = Maps.newHashMap();

    /**
     * Call if the handler methods need to return a value.
     *
     * @param returnType the type of the return value.
     * @param <R>  The type of the return value
     * @return the builder.
     */
    @SuppressWarnings("unused")
    public <R> BuilderR2<K, A0, A1, R> withReturnType(final Class<R> returnType) {
      if (!handlers.isEmpty()) {
        throw new IllegalStateException("handlers already defined");
      }
      return new BuilderR2<>();
    }

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
    public ClassHandlerMap2<K, A0, A1> build() {
      return new ClassHandlerMap2<>(handlers);
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

  public static final class ClassHandlerMap2<K, A0, A1> {

    private final Map<Class<? extends K>, Handler2<K, A0, A1>> handlers;

    private ClassHandlerMap2(final Map<Class<? extends K>, Handler2<K, A0, A1>> handlers) {
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

    public Set<Class<? extends K>> keySet() {
      return handlers.keySet();
    }
  }

  public static final class BuilderR2<K, A0, A1, R> {

    private final Map<Class<? extends K>, HandlerR2<K, A0, A1, R>> handlers = Maps.newHashMap();

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
    public <KT extends K> BuilderR2<K, A0, A1, R> put0(
        final Class<KT> type,
        final Supplier<? extends HandlerR0<? super KT, ? extends R>> supplier) {

      final HandlerR0<? super KT, ? extends R> handler;

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
    public <KT extends K> BuilderR2<K, A0, A1, R> put1(
        final Class<KT> type,
        final Supplier<? extends HandlerR1<? super KT, ? super A0, ? extends R>> supplier) {

      final HandlerR1<? super KT, ? super A0, ? extends R> handler;

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
    public <KT extends K> BuilderR2<K, A0, A1, R> put(
        final Class<KT> type,
        final Supplier<? extends HandlerR2<? super KT, ? super A0, ? super A1, ? extends R>>
            supplier) {

      final HandlerR2<? super KT, ? super A0, ? super A1, ? extends R> handler;

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
    public <KT extends K> BuilderR2<K, A0, A1, R> put(
        final Class<KT> type,
        final HandlerR0<? super KT, ? extends R> handler) {

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
    public <KT extends K> BuilderR2<K, A0, A1, R> put(
        final Class<KT> type,
        final HandlerR1<? super KT, ? super A0, ? extends R> handler) {

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
    public <KT extends K> BuilderR2<K, A0, A1, R> put(
        final Class<KT> type,
        final HandlerR2<? super KT, ? super A0, ? super A1, ? extends R> handler) {

      if (handlers.containsKey(type)) {
        throw new IllegalArgumentException("Duplicate key: " + type);
      }
      handlers.put(type, castHandler2(type, handler));
      return this;
    }

    /**
     * @return the built handler map.
     */
    public ClassHandlerMapR2<K, A0, A1, R> build() {
      return new ClassHandlerMapR2<>(handlers);
    }

    private <KT extends K> HandlerR2<K, A0, A1, R> castHandler0(
        final Class<KT> keyType,
        final HandlerR0<? super KT, ? extends R> handler) {

      return (a0, a1, k) -> handler.handle(keyType.cast(k));
    }

    private <KT extends K> HandlerR2<K, A0, A1, R> castHandler1(
        final Class<KT> keyType,
        final HandlerR1<? super KT, ? super A0, ? extends R> handler) {

      return (a0, a1, k) -> handler.handle(a0, keyType.cast(k));
    }

    private <KT extends K> HandlerR2<K, A0, A1, R> castHandler2(
        final Class<KT> keyType,
        final HandlerR2<? super KT, ? super A0, ? super A1, ? extends R> handler) {

      return (a0, a1, k) -> handler.handle(a0, a1, keyType.cast(k));
    }
  }

  public static final class ClassHandlerMapR2<K, A0, A1, R> {

    private final Map<Class<? extends K>, HandlerR2<K, A0, A1, R>> handlers;

    private ClassHandlerMapR2(final Map<Class<? extends K>, HandlerR2<K, A0, A1, R>> handlers) {
      this.handlers = ImmutableMap.copyOf(handlers);
    }

    public HandlerR2<K, A0, A1, R> get(final Class<? extends K> type) {
      return handlers.get(type);
    }

    @SuppressWarnings("unchecked")
    public <KT extends K> HandlerR2<KT, A0, A1, R> getTyped(final Class<KT> type) {
      return (HandlerR2<KT, A0, A1, R>) handlers.get(type);
    }

    @SuppressWarnings("unchecked")
    public HandlerR2<K, A0, A1, R> getOrDefault(
        final Class<? extends K> type,
        final HandlerR2<? super K, ? super A0, ? super A1, ? extends R> defaultHandler) {
      return handlers.getOrDefault(type, (HandlerR2<K, A0, A1, R>) defaultHandler);
    }

    public Set<Class<? extends K>> keySet() {
      return handlers.keySet();
    }
  }
}
