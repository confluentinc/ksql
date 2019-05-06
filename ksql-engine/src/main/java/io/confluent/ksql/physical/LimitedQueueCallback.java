/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.physical;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@code LimitQueueCallback} that limits the number of events queued and fires the
 * {@code LimitHandler} when the limit is reached.
 */
@ThreadSafe
public final class LimitedQueueCallback implements LimitQueueCallback {

  private final AtomicInteger remaining;
  private final AtomicInteger queued;
  private volatile LimitHandler limitHandler = () -> {
  };

  LimitedQueueCallback(final int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be positive, was:" + limit);
    }
    this.remaining = new AtomicInteger(limit);
    this.queued = new AtomicInteger(limit);
  }

  @Override
  public void setLimitHandler(final LimitHandler limitHandler) {
    this.limitHandler = Objects.requireNonNull(limitHandler, "limitHandler");
  }

  @Override
  public boolean shouldQueue() {
    return remaining.decrementAndGet() >= 0;
  }

  @Override
  public void onQueued() {
    if (queued.decrementAndGet() == 0) {
      limitHandler.limitReached();
    }
  }
}
