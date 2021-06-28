/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api.server;

import io.confluent.ksql.util.Pair;
import java.util.LinkedList;
import java.util.Queue;

public class SlidingWindowRateLimiter {
  private final Queue<Pair<Long, Long>> queue;  // <timestamp, bytes>
  private static final long requestLimit = 1000;
  private static final long timeLimit = 1000 * 60 * 60;  //1 hour in miliseconds
  private long lastHourBytes;

  public SlidingWindowRateLimiter() {
    this.queue = new LinkedList<>();
    this.lastHourBytes = 0;
  }

  public boolean allow(final long timestamp) {
    while (!queue.isEmpty() && queue.peek().left - timestamp >= timeLimit) {
      this.lastHourBytes -= queue.poll().right;
    }
    if (this.lastHourBytes < requestLimit) {
      return true;
    }
    return false;
  }

  public void add(final long timestamp, final long bytes) {
    queue.add(new Pair<>(timestamp, bytes));
    this.lastHourBytes += bytes;
  }
}