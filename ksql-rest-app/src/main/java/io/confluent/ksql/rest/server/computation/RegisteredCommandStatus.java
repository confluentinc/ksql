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

import java.util.concurrent.CompletableFuture;

public class RegisteredCommandStatus {
  private final CommandId commandId;
  private volatile CommandStatus commandStatus;
  private final CompletableFuture<CommandStatus> future;

  public RegisteredCommandStatus(
      final CommandId commandId, final CommandStatus initialStatus) {
    this.commandId = commandId;
    this.commandStatus = initialStatus;
    this.future = new CompletableFuture<>();
  }

  public CommandStatus getCurrentStatus() {
    return commandStatus;
  }

  public void setCurrentStatus(final CommandStatus status) {
    this.commandStatus = status;
  }

  public CommandId getCommandId() {
    return commandId;
  }

  public CompletableFuture<CommandStatus> getFuture() {
    return future;
  }
}