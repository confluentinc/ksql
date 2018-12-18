/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.computation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

class SequenceNumberFutureStore {
  private final ConcurrentHashMap<Long, CompletableFuture<Void>> sequenceNumberFutures;
  private long lastCompletedSequenceNumber;

  SequenceNumberFutureStore() {
    sequenceNumberFutures = new ConcurrentHashMap<>();
    lastCompletedSequenceNumber = -1;
  }

  synchronized CompletableFuture<Void> getFutureForSequenceNumber(final long seqNum) {
    if (seqNum <= lastCompletedSequenceNumber) {
      return CompletableFuture.completedFuture(null);
    }
    return sequenceNumberFutures.computeIfAbsent(seqNum, k -> new CompletableFuture<>());
  }

  void completeFuturesUpToAndIncludingSequenceNumber(final long seqNum) {
    synchronized (this) {
      lastCompletedSequenceNumber = seqNum;
    }
    sequenceNumberFutures.keySet().stream()
        .filter(k -> k <= seqNum)
        .forEach(k -> {
          sequenceNumberFutures.get(k).complete(null);
          sequenceNumberFutures.remove(k);
        });
  }
}
