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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class QueuedCommandStatus {
  private static final CommandStatus INITIAL_STATUS = new CommandStatus(
      CommandStatus.Status.QUEUED, "Statement written to command topic");

  private final CommandId commandId;
  private volatile CommandStatus commandStatus;
  private final CompletableFuture<CommandStatus> future;

  public QueuedCommandStatus(final CommandId commandId) {
    this.commandId = Objects.requireNonNull(commandId);
    this.commandStatus = INITIAL_STATUS;
    this.future = new CompletableFuture<>();
  }

  public CommandId getCommandId() {
    return commandId;
  }

  public CommandStatus getStatus() {
    return commandStatus;
  }

  public CommandStatus tryWaitForFinalStatus(final long timeout, final TimeUnit timeUnit)
      throws InterruptedException {
    try {
      return future.get(timeout, timeUnit);
    } catch (final ExecutionException e) {
      throw new RuntimeException("Error executing command " + commandId, e.getCause());
    } catch (final TimeoutException e) {
      return commandStatus;
    }
  }

  public void setStatus(final CommandStatus status) {
    this.commandStatus = Objects.requireNonNull(status);
  }

  public void setFinalStatus(final CommandStatus status) {
    setStatus(status);
    future.complete(status);
  }
}