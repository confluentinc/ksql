package io.confluent.ksql.physical.scalablepush.consumer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class CatchupCoordinatorImpl implements CatchupCoordinator {

  private final AtomicInteger blockers = new AtomicInteger(0);
  private final AtomicBoolean waiting = new AtomicBoolean(false);

  @Override
  public void checkShouldWaitForCatchup() {
    synchronized (blockers) {
      while (blockers.get() > 0) {
        try {
          System.out.println("WAITING TO BE WOKEN UP");
          waiting.set(true);
          blockers.wait();
          System.out.println("WAKING UP");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      waiting.set(false);
    }
  }

  @Override
  public boolean checkShouldCatchUp(
      final AtomicBoolean blocked,
      final Supplier<Boolean> isCaughtUp,
      final Runnable switchOver
  ) {
    // Check caught up first before grabbing the lock
    if (isCaughtUp.get()) {
      System.out.println("CAUGHT UP!!");
      synchronized (blockers) {
        if (waiting.get() && isCaughtUp.get()) {
          System.out.println("TRANSFER");
          if (blocked.get()) {
            System.out.println("DECREMENTING BLOCKERS to " + blockers.get());
            blockers.decrementAndGet();
            blockers.notify();
          }
          switchOver.run();
          return true;
        } else {
          System.out.println("INCREMENTING BLOCKERS to " + blockers.get());
          blocked.set(true);
          blockers.incrementAndGet();
        }
      }
    }
    return false;
  }
}
