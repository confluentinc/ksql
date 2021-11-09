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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class CatchupCoordinatorImpl implements CatchupCoordinator {

  private final AtomicInteger catchupJoiners = new AtomicInteger(0);
  private final AtomicBoolean latestWaiting = new AtomicBoolean(false);

  @Override
  public void checkShouldWaitForCatchup() {
    synchronized (catchupJoiners) {
      while (catchupJoiners.get() > 0) {
        try {
          System.out.println("WAITING TO BE WOKEN UP");
          latestWaiting.set(true);
          catchupJoiners.wait();
          System.out.println("WAKING UP catchupJoiners.get()" + catchupJoiners.get());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      latestWaiting.set(false);
    }
  }

  @Override
  public boolean checkShouldCatchUp(
      final AtomicBoolean signalledLatest,
      final Supplier<Boolean> isCaughtUp,
      final Runnable switchOver
  ) {
    // Check caught up first before grabbing the lock
    if (isCaughtUp.get()) {
      System.out.println("CAUGHT UP!!");
      synchronized (catchupJoiners) {
        if (latestWaiting.get() && isCaughtUp.get()) {
          System.out.println("TRANSFER");
          if (signalledLatest.get()) {
            System.out.println("DECREMENTING BLOCKERS to " + catchupJoiners.get());
            catchupJoiners.decrementAndGet();
            catchupJoiners.notify();
          }
          switchOver.run();
          return true;
        } else if (!signalledLatest.get()) {
          System.out.println("INCREMENTING BLOCKERS to " + catchupJoiners.get());
          signalledLatest.set(true);
          catchupJoiners.incrementAndGet();
        }
      }
    }
    return false;
  }

  @VisibleForTesting
  public void simulateWaitingInTest() {
    synchronized (catchupJoiners) {
      if (catchupJoiners.get() > 0) {
        latestWaiting.set(true);
      }
    }
  }
}
