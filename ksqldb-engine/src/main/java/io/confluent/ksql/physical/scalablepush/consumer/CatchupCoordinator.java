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
   * @param signalledLatest If the catchup thread has attempted to signal to latest so that it can
   *                join it. Should start off false an implementations can set it if appropriate.
   * @param isCaughtUp If the catchup thread is caught up.
   * @param switchOver After the switch over can go forward, this logic must implement it.
   * @return If the switch over has occurred
   */
  boolean checkShouldCatchUp(
      AtomicBoolean signalledLatest,
      Supplier<Boolean> isCaughtUp,
      Runnable switchOver
  );
}
