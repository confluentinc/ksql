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

/**
 * Type-safe collection of handlers for different types.
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

  @SuppressWarnings("unchecked")
  public <TT extends T> Handler<TT, A> get(final Class<TT> type) {
    return (Handler<TT, A>) handlers.get(type);
  }

  @SuppressWarnings("unchecked")
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

    public <TT extends T> Builder<T, A> put(
        final Class<TT> type,
        final Handler<? super TT, A> handler) {

      handlers.put(type, castHandler(type, handler));
      return this;
    }

    public <TT extends T> Builder<T, A> put(
        final Class<TT> type,
        final Class<? extends Handler<? super TT, A>> handlerClass) {

      try {
        return put(type, handlerClass.newInstance());
      } catch (final Exception e) {
        throw new RuntimeException("Failed to instantiate handler: " + handlerClass, e);
      }
    }

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
