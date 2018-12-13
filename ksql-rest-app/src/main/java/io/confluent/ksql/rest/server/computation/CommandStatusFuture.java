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

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.rest.entity.CommandStatus;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CommandStatusFuture {
  private static final CommandStatus INITIAL_STATUS = new CommandStatus(
      CommandStatus.Status.QUEUED, "Statement written to command topic");

  private final CommandId commandId;
  private volatile CommandStatus currentStatus;
  private final CompletableFuture<CommandStatus> finalStatusFuture;

  public CommandStatusFuture(final CommandId commandId) {
    this.commandId = Objects.requireNonNull(commandId, "commandId cannot be null");
    this.currentStatus = INITIAL_STATUS;
    this.finalStatusFuture = new CompletableFuture<>();
  }

  CommandId getCommandId() {
    return commandId;
  }

  CommandStatus getStatus() {
    return currentStatus;
  }

  CommandStatus tryWaitForFinalStatus(final Duration timeout) throws InterruptedException {
    try {
      return finalStatusFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException)e.getCause();
      }
      throw new RuntimeException("Error executing command " + commandId, e.getCause());
    } catch (final TimeoutException e) {
      return currentStatus;
    }
  }

  void setStatus(final CommandStatus status) {
    this.currentStatus = Objects.requireNonNull(status);
  }

  void setFinalStatus(final CommandStatus status) {
    setStatus(status);
    finalStatusFuture.complete(status);
  }
}
