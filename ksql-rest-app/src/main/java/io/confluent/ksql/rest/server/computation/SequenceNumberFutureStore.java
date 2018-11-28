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

public class SequenceNumberFutureStore {
  private final ConcurrentHashMap<Long, CompletableFuture<Void>> sequenceNumberFutures;

  public SequenceNumberFutureStore() {
    sequenceNumberFutures = new ConcurrentHashMap<>(8, 0.9f, 1);
  }

  public CompletableFuture<Void> getFutureForSequenceNumber(final long seqNum) {
    return sequenceNumberFutures.computeIfAbsent(seqNum, k -> new CompletableFuture<>());
  }

  public void completeFuturesUpToSequenceNumber(final long seqNum) {
    sequenceNumberFutures.keySet().stream()
        .filter(k -> k < seqNum)
        .forEach(k -> {
          sequenceNumberFutures.get(k).complete(null);
          sequenceNumberFutures.remove(k);
        });
  }
}
