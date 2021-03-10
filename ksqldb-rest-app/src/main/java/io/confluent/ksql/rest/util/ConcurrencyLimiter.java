/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.util.KsqlException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrencyLimiter {

  private final int limit;
  private final String operationType;

  private AtomicInteger count = new AtomicInteger(0);

  public ConcurrencyLimiter(final int limit, final String operationType) {
    this.limit = limit;
    this.operationType = operationType;
  }

  public Decrementer increment() {
    count.updateAndGet(value -> {
      if (value + 1 > limit) {
        throw new KsqlException(
            String.format("Host is at concurrency limit for %s. Currently set to %d maximum "
                + "concurrent operations.", operationType, limit));
      }
      return value + 1;
    });
    return new Decrementer();
  }

  private void decrement() {
    count.decrementAndGet();
  }

  @VisibleForTesting
  int getCount() {
    return count.get();
  }

  public class Decrementer {

    private final AtomicBoolean called = new AtomicBoolean(false);

    public void decrementAtMostOnce() {
      if (!called.getAndSet(true)) {
        ConcurrencyLimiter.this.decrement();
      }
    }
  }
}
