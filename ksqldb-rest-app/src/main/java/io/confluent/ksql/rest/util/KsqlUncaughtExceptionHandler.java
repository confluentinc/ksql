/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.rest.server.KsqlServerMain;
import io.confluent.ksql.util.JavaSystemExit;
import io.confluent.ksql.util.SystemExit;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KsqlUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
  private final Runnable flusher;
  private final Optional<CountDownLatch> countDownLatch;

  private static final Logger log = LoggerFactory.getLogger(KsqlServerMain.class);

  public KsqlUncaughtExceptionHandler(final Runnable flusher) {
    this(flusher, Optional.empty());
  }

  @VisibleForTesting
  public KsqlUncaughtExceptionHandler(
      final Runnable flusher,
      final Optional<CountDownLatch> countDownLatch
  ) {
    this.flusher = flusher;
    this.countDownLatch = countDownLatch;
  }

  @SuppressFBWarnings
  public void uncaughtException(final Thread t, final Throwable e) {
    uncaughtExceptionInternal(t, e, new JavaSystemExit());
  }

  void uncaughtExceptionInternal(
      final Thread t,
      final Throwable e,
      final SystemExit systemExit) {
    if (t instanceof StreamThread) {
      countDownLatch.ifPresent(CountDownLatch::countDown);
      return;
    }
    log.error("Unhandled exception caught in thread {}.", t.getName(), e);
    System.err.println(
        "Unhandled exception caught in thread: " + t.getName() + ". Exception:" + e.getMessage());

    flusher.run();

    systemExit.exit(-1);
  }
}
