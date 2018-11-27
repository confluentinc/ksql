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

package io.confluent.ksql.rest.server.computation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetFutureStore {
  private final ConcurrentHashMap<Long, CompletableFuture<Void>> offsetFutures;

  public OffsetFutureStore() {
    offsetFutures = new ConcurrentHashMap<>(8, 0.9f, 1);
  }

  public CompletableFuture<Void> getFutureForOffset(final long offset) {
    return offsetFutures.computeIfAbsent(offset, k -> new CompletableFuture<>());
  }

  public void completeFuturesUpToOffset(final long offset) {
    offsetFutures.keySet().stream()
        .filter(k -> k < offset)
        .forEach(k -> {
          offsetFutures.get(k).complete(null);
          offsetFutures.remove(k);
        });
  }
}
