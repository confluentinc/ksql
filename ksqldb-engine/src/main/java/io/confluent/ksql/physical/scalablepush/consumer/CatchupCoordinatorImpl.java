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

package io.confluent.ksql.physical.scalablepush.consumer;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.engine.QueryCleanupService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatchupCoordinatorImpl implements CatchupCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(CatchupCoordinatorImpl.class);

  private final AtomicInteger catchupJoiners = new AtomicInteger(0);
  private final AtomicBoolean latestWaiting = new AtomicBoolean(false);

  @Override
  public synchronized void checkShouldWaitForCatchup() {
      while (catchupJoiners.get() > 0) {
        try {
          latestWaiting.set(true);
          this.wait();
        } catch (InterruptedException e) {
          LOG.error("Caught InterruptedException during catchup waiting", e);
          Thread.currentThread().interrupt();
        }
      }
      latestWaiting.set(false);
  }

  @Override
  public synchronized boolean checkShouldCatchUp(
      final AtomicBoolean signalledLatest,
      final Supplier<Boolean> isCaughtUp,
      final Runnable switchOver
  ) {
    // Check caught up first before grabbing the lock
    if (latestWaiting.get() && isCaughtUp.get()) {
      LOG.info("Catchup is joining latest");
      if (signalledLatest.get()) {
        catchupJoiners.decrementAndGet();
        this.notify();
      }
      switchOver.run();
      return true;
    } else if (!signalledLatest.get()) {
      signalledLatest.set(true);
      catchupJoiners.incrementAndGet();
    }
    return false;
  }

  @VisibleForTesting
  public synchronized void simulateWaitingInTest() {
    if (catchupJoiners.get() > 0) {
      latestWaiting.set(true);
    }
  }
}
