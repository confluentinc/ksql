/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.rest.server.computation;

import org.apache.kafka.common.utils.Utils;

import io.confluent.ksql.rest.entity.CommandStatus;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;


public class CommandStatusFuture implements Future<CommandStatus> {
  interface Cleanup {
    void run(CommandId commandId);
  }

  private final CommandId commandId;
  private Cleanup cleanup;
  private final AtomicReference<CommandStatus> result;

  public CommandStatusFuture(CommandId commandId, Cleanup cleanup) {
    this.commandId = commandId;
    this.cleanup = cleanup;
    this.result = new AtomicReference<>(null);
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return result.get() != null;
  }

  @Override
  public CommandStatus get() throws InterruptedException {
    while (result.get() == null) {
      result.wait();
    }
    cleanup.run(commandId);
    return result.get();
  }

  @Override
  public CommandStatus get(long timeout, TimeUnit unit)
          throws InterruptedException, TimeoutException {
    long endTimeMs = System.currentTimeMillis() + unit.toMillis(timeout);
    while (System.currentTimeMillis() < endTimeMs && result.get() == null) {
      Utils.sleep(1);
    }
    if (result.get() == null) {
      throw new TimeoutException();
    }
    cleanup.run(commandId);
    return result.get();
  }

  public void complete(CommandStatus result) {
    this.result.set(result);
  }
}