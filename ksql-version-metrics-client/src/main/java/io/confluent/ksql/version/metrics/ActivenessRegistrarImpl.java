/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.version.metrics;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ActivenessRegistrarImpl implements ActivenessRegistrar, Supplier<Boolean> {

  // 24 hours
  private static final long MAX_INTERVAL = TimeUnit.DAYS.toMillis(1);

  private long requestTime;
  private boolean hasActiveQuery;

  public ActivenessRegistrarImpl() {
    this.requestTime = System.currentTimeMillis();
    this.hasActiveQuery = false;
  }

  // Only for test purposes.
  public ActivenessRegistrarImpl(final long requestTime) {
    this.requestTime = requestTime;
    this.hasActiveQuery = false;
  }

  @Override
  public void fire(final boolean hasActiveQuery) {
    this.requestTime = System.currentTimeMillis();
    this.hasActiveQuery = hasActiveQuery;
  }

  @Override
  public Boolean get() {
    return (System.currentTimeMillis() - this.requestTime) < MAX_INTERVAL || hasActiveQuery;
  }
}
