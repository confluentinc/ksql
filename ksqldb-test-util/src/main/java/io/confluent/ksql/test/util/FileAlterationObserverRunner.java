/*
 * Copyright 2018 Confluent Inc.
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.monitor.FileAlterationObserver;

public class FileAlterationObserverRunner implements Runnable {
  private final FileAlterationObserver fileAlterationObserver;

  private final AtomicBoolean isRunning;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public FileAlterationObserverRunner(
      final FileAlterationObserver fileAlterationObserver,
      final AtomicBoolean isRunning
  ) {
    this.fileAlterationObserver = fileAlterationObserver;
    this.isRunning = isRunning;
  }

  @Override
  public void run() {
    try {
      fileAlterationObserver.initialize();
      while (isRunning.get()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        fileAlterationObserver.checkAndNotify();
      }
    } catch (Exception e) {
      System.out.println("Error while running file observer.");
    }
  }
}