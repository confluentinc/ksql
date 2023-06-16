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

public class NoopCatchupCoordinator implements CatchupCoordinator {

  @Override
  public void checkShouldWaitForCatchup() {
  }

  @Override
  public boolean checkShouldCatchUp(
      final AtomicBoolean blocked,
      final Function<Boolean, Boolean> isCaughtUp,
      final Runnable switchOver) {
    return false;
  }

  @Override
  public void catchupIsClosing(final AtomicBoolean signalledLatest) {
  }
}
