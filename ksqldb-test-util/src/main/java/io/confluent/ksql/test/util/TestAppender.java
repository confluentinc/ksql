/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.ksql.test.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class TestAppender extends AppenderSkeleton {
  private final List<LoggingEvent> log = new ArrayList<>();

  @Override
  public boolean requiresLayout() {
    return false;
  }

  @Override
  public void append(final LoggingEvent loggingEvent) {
    // Force the MDC snapshot now, while still on the logging thread with the
    // context still populated.
    loggingEvent.getMDCCopy();
    log.add(loggingEvent);
  }

  @Override
  public void close() {
  }

  public List<LoggingEvent> getLog() {
    return new ArrayList<>(log);
  }
}