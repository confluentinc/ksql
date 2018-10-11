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

import io.confluent.support.metrics.common.time.TimeUtils;
import java.util.concurrent.TimeUnit;

public class KsqlServerActiveCheckerImpl implements ActiveChecker {

  // 24 hours
  private static final long MAX_INTERVAL = TimeUnit.DAYS.toMillis(1);

  private long requestTime;
  private boolean hasActiveQuery;
  private final TimeUtils timeUtils;

  public KsqlServerActiveCheckerImpl() {
    timeUtils = new TimeUtils();
    requestTime = timeUtils.nowInUnixTime();
    hasActiveQuery = false;
  }

  @Override
  public void onRequest(final long requestTime, final boolean hasActiveQuery) {
    this.requestTime = requestTime;
    this.hasActiveQuery = hasActiveQuery;
  }

  @Override
  public boolean isActive() {
    final long lastInterval = timeUtils.nowInUnixTime() - requestTime;
    return lastInterval < MAX_INTERVAL || hasActiveQuery;
  }
}
