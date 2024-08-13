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

package io.confluent.ksql.execution.scalablepush.consumer;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatchupCoordinatorImpl implements CatchupCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(CatchupCoordinatorImpl.class);
  // The maximum amount of time waited.  This ensures that we don't freeze the latest indefinitely
  // and the latest can still potentially be closed.
  private static final long WAIT_TIME_MS = 10000;
  private final Clock clock;

  private int catchupJoiners = 0;
  private boolean latestWaiting = false;

  public CatchupCoordinatorImpl() {
    this(Clock.systemUTC());
  }

  public CatchupCoordinatorImpl(final Clock clock) {
    this.clock = clock;
  }

  @Override
  public synchronized void checkShouldWaitForCatchup() {
    // If any catchup joiners are trying to catch up with us, we set latestWaiting and then wait
    // on this object to be notified once the switchover is complete.
    final long startTime = clock.millis();
    while (catchupJoiners > 0) {
      try {
        latestWaiting = true;
        LOG.info("Waiting for Catchups to join Latest consumer, current count {}", catchupJoiners);
        this.wait(WAIT_TIME_MS);
        final long waitedMs = clock.millis() - startTime;
        LOG.info("Waited for Catchups to join Latest consumer for {}ms and current count {}",
            waitedMs, catchupJoiners);
        if (waitedMs >= WAIT_TIME_MS) {
          break;
        }
      } catch (InterruptedException e) {
        LOG.error("Caught InterruptedException during catchup waiting", e);
        Thread.currentThread().interrupt();
        throw new RuntimeException("InterruptedException during catchup waiting", e);
      }
    }
    latestWaiting = false;
  }

  @Override
  public synchronized boolean checkShouldCatchUp(
      final AtomicBoolean signalledLatest,
      final Function<Boolean, Boolean> isCaughtUp,
      final Runnable switchOver
  ) {
    // If latest is waiting already, we check again that we're caught up, but not a soft check.
    // This means that latest has been waiting and we're actually fully, message for message caught
    // up (and may even be a few past).
    if (latestWaiting && isCaughtUp.apply(false)) {
      LOG.info("Catchup is joining latest, about to decrement {}", catchupJoiners);
      if (signalledLatest.get()) {
        signalledLatest.set(false);
        catchupJoiners--;
        this.notify();
      }
      switchOver.run();
      return true;

      // If we haven't yet signalled that we're ready to switch over by incrementing catchupJoiners,
      // and we're soft caught up, then we tell latest to pause by incrementing catchupJoiners.
    } else if (!signalledLatest.get() && isCaughtUp.apply(true)) {
      LOG.info("Signalling Latest, about to increment {}", catchupJoiners);
      signalledLatest.set(true);
      catchupJoiners++;
    }
    return false;
  }

  @Override
  public synchronized void catchupIsClosing(
      final AtomicBoolean signalledLatest
  ) {
    if (signalledLatest.get()) {
      signalledLatest.set(false);
      catchupJoiners--;
    }
  }

  @VisibleForTesting
  public synchronized void simulateWaitingInTest() {
    if (catchupJoiners > 0) {
      latestWaiting = true;
    }
  }
}
