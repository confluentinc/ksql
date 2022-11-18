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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Interface for coordinating a latest thread and any number of catchup threads which will pause
 * and then join with the primary thread. This is done by getting soft caught up with the latest,
 * which effectively means within some number of messages of latest, in which case latest is paused
 * and the catchup will then get the rest of the way, joining up and then unpausing the latest.
 * This relies on the ability to catchup with latest, which means that data is not being produced
 * so fast that this cannot be done. If it were, even latest would have an impossible time to
 * actually catch up with the latest messages.
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
   * @param signalledLatest If the catchup thread has attempted to signal to latest so that it can
   *                join it. Should start off false an implementations can set it if appropriate.
   * @param isCaughtUp If the catchup thread is caught up, takes a parameter softCatchUp which
   *                   indicates if we should check for a "soft" comparison that is within a
   *                   defined offset margin.
   * @param switchOver After the switch over can go forward, this logic must implement it.
   * @return If the switch over has occurred
   */
  boolean checkShouldCatchUp(
      AtomicBoolean signalledLatest,
      Function<Boolean, Boolean> isCaughtUp,
      Runnable switchOver
  );

  /**
   * When the catchup thread is closing, we want to ensure that if it signaled that it was waiting
   * but never made the switch, that we un-signal it as waiting.
   * @param signalledLatest The indicator if it signaled to latest that it was waiting.
   */
  void catchupIsClosing(AtomicBoolean signalledLatest);
}
