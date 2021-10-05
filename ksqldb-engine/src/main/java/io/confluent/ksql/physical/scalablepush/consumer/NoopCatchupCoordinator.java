package io.confluent.ksql.physical.scalablepush.consumer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class NoopCatchupCoordinator implements CatchupCoordinator {

  @Override
  public void checkShouldWaitForCatchup() {
  }

  @Override
  public boolean checkShouldCatchUp(
      final AtomicBoolean blocked,
      final Supplier<Boolean> isCaughtUp,
      final Runnable switchOver) {
    return false;
  }
}
