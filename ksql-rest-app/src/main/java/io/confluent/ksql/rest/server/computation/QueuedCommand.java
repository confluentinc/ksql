/**
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
 **/

package io.confluent.ksql.rest.server.computation;

import java.util.Objects;
import java.util.Optional;

public class QueuedCommand {
  private final CommandId commandId;
  private final Command command;
  private final Optional<QueuedCommandStatus> status;

  public QueuedCommand(final CommandId commandId,
                       final Command command,
                       final Optional<QueuedCommandStatus> status) {
    this.commandId = Objects.requireNonNull(commandId);
    this.command = Objects.requireNonNull(command);
    this.status = Objects.requireNonNull(status);
  }

  QueuedCommand(final CommandId commandId, final Command command) {
    this(commandId, command, Optional.empty());
  }

  public CommandId getCommandId() {
    return commandId;
  }

  public Optional<QueuedCommandStatus> getStatus() {
    return status;
  }

  public Command getCommand() {
    return command;
  }

  @Override
  public int hashCode() {
    return Objects.hash(commandId, command, status);
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if ((other == null) || (getClass() != other.getClass())) {
      return false;
    }
    QueuedCommand otherQueuedCommand = (QueuedCommand) other;
    return this.command.equals(otherQueuedCommand.getCommand())
        && this.commandId.equals(otherQueuedCommand.getCommandId())
        && this.status.equals(otherQueuedCommand.getStatus());

  }
}
