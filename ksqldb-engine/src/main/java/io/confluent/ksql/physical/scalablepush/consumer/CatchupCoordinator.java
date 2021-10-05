package io.confluent.ksql.physical.scalablepush.consumer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Interface for coordinating a primary thread and any number of catchup threads which will pause
 * and then join with the primary thread.
 */
public interface CatchupCoordinator {

  /**
   * Checks to see if the primary thread should wait for any catchup threads, and then waits if
   * appropriate.
   */
  void checkShouldWaitForCatchup();

  /**
   * Coordinates whether the primary thread is ready to be caught up with and calls switchOver
   * when appropriate.
   * @param blocked If the catchup thread has attempted to block the primary thread so that it can
   *                join it. Should start off false an implementations can set it if appropriate.
   * @param isCaughtUp If the catchup thread is caught up.
   * @param switchOver After the switch over can go forward, this logic must implement it.
   * @return
   */
  boolean checkShouldCatchUp(
      final AtomicBoolean blocked,
      final Supplier<Boolean> isCaughtUp,
      final Runnable switchOver);
}
