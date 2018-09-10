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

import io.confluent.ksql.rest.entity.CommandStatus;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CommandStatusFuture implements Future<CommandStatus> {
  private final CommandId commandId;
  private boolean completed;
  private CommandStatus result;

  public CommandStatusFuture(
      final CommandId commandId, final CommandStatus initialStatus) {
    this.commandId = commandId;
    this.completed = false;
    this.result = initialStatus;
  }

  @Override
  public boolean cancel(final boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return completed;
  }

  @Override
  public synchronized CommandStatus get() throws InterruptedException {
    while (!completed) {
      wait();
    }
    return result;
  }

  @Override
  public synchronized CommandStatus get(final long timeout, final TimeUnit unit)
          throws InterruptedException {
    final long endTimeMs = System.currentTimeMillis() + unit.toMillis(timeout);
    while (System.currentTimeMillis() < endTimeMs && !completed) {
      wait(Math.max(endTimeMs - System.currentTimeMillis(), 1));
    }
    return result;
  }

  public synchronized CommandStatus getCurrentStatus() {
    return result;
  }

  public CommandId getCommandId() {
    return commandId;
  }

  public synchronized void update(final CommandStatus result) {
    this.result = result;
  }

  public synchronized void complete() {
    this.completed = true;
    notifyAll();
  }
}