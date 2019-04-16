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
import java.util.function.Supplier;

/**
 * Type-safe collection of handlers for different types.
 *
 * <p>Can be used to build a static map of handlers for certain types, e.g.
 * <pre>
 * {@code
 * class SomeClass {
 *   private static final HandlerMap<SomeBaseType, SomeClass> HANDLERS =
 *       HandlerMap.<SomeBaseType, SomeClass>builder()
 *           .put(SomeDerivedType.class, SomeClass::someMethod)
 *           .put(SomeOtherDerivedType.class, SomeClass::someOtherMethod)
 *           .build();
 * }
 * }
 * </pre>
 *
 * @param <T> the type to be handled.
 * @param <A> the type of the argument that is passed to the handler along with the type.
 */
public final class HandlerMap<T, A> {
  private final Map<Class<? extends T>, Handler<T, A>> handlers;

  private HandlerMap(final Map<Class<? extends T>, Handler<T, A>> handlers) {
    this.handlers = ImmutableMap.copyOf(handlers);
  }

  @FunctionalInterface
  public interface Handler<T, A> {
    void handle(A arg, T type);
  }

  public Handler<T, A> get(final Class<? extends T> type) {
    return handlers.get(type);
  }

  @SuppressWarnings("unchecked")
  public <TT extends T> Handler<TT, A> getTyped(final Class<TT> type) {
    return (Handler<TT, A>) handlers.get(type);
  }

  public Handler<T, A> getOrDefault(
      final Class<? extends T> type,
      final Handler<T, A> defaultHandler) {
    return handlers.getOrDefault(type, defaultHandler);
  }

  public static <T, A> Builder<T, A> builder() {
    return new Builder<>();
  }

  public static class Builder<T, A> {
    private final Map<Class<? extends T>, Handler<T, A>> handlers = Maps.newHashMap();

    /**
     * Add a new handler to the map.
     *
     * @param type the key type to register the handler against
     * @param handler the handler to the supplied {@code type}
     * @param <TT> the derived type.
     * @return set.
     */
    public <TT extends T> Builder<T, A> put(
        final Class<TT> type,
        final Handler<? super TT, A> handler) {

      if (handlers.containsKey(type)) {
        throw new IllegalArgumentException("Duplicate key: " + type);
      }
      handlers.put(type, castHandler(type, handler));
      return this;
    }

    /**
     * Allows the passing of handlers via lambdas.
     *
     * <p>e.g. put(Some.class, SomeHandler::new)
     *
     * <p>Which means SomeHandler does not count towards the classes data abstraction coupling
     *
     * @param type the key type to register the handler against
     * @param supplier the supplier of the handler. The supplier will be called immediately.
     * @param <TT> the derived type.
     * @return set.
     */
    public <TT extends T> Builder<T, A> put(
        final Class<TT> type,
        final Supplier<? extends Handler<? super TT, A>> supplier) {

      final Handler<? super TT, A> handler;

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
    public HandlerMap<T, A> build() {
      return new HandlerMap<>(handlers);
    }

    private <TT extends T> Handler<T, A> castHandler(
        final Class<TT> type,
        final Handler<? super TT, A> handler) {

      return (a, t) -> handler.handle(a, type.cast(t));
    }
  }
}
