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

package io.confluent.ksql.rest.server.computation;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.rest.entity.CommandId;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.serialization.Deserializer;

public class QueuedCommand {
  private final byte[] commandId;
  private final byte[] command;
  private final Optional<CommandStatusFuture> status;
  private final Long offset;

  @VisibleForTesting
  public QueuedCommand(
      final CommandId commandId,
      final Command command,
      final Optional<CommandStatusFuture> status,
      final Long offset
  ) {
    this(
        InternalTopicSerdes.serializer().serialize("", commandId),
        InternalTopicSerdes.serializer().serialize("", command),
        status,
        offset
    );
  }

  public QueuedCommand(
      final byte[] commandId,
      final byte[] command,
      final Optional<CommandStatusFuture> status,
      final Long offset
  ) {
    this.commandId = Objects.requireNonNull(commandId, "commandId");
    this.command = Objects.requireNonNull(command,"command");
    this.status = Objects.requireNonNull(status, "status");
    this.offset = Objects.requireNonNull(offset, "offset");
  }

  @VisibleForTesting
  byte[] getCommandId() {
    return Arrays.copyOf(commandId, commandId.length);
  }

  @VisibleForTesting
  byte[] getCommand() {
    return  Arrays.copyOf(command, command.length);
  }

  public CommandId getAndDeserializeCommandId() {
    return InternalTopicSerdes.deserializer(CommandId.class).deserialize("", commandId);
  }

  public Command getAndDeserializeCommand(final Deserializer<Command> deserializer) {
    return deserializer.deserialize("", command);
  }

  public Optional<CommandStatusFuture> getStatus() {
    return status;
  }

  public Long getOffset() {
    return offset;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final QueuedCommand that = (QueuedCommand) o;
    return Arrays.equals(commandId, that.commandId)
        && Arrays.equals(command, that.command)
        && Objects.equals(status, that.status)
        && Objects.equals(offset, that.offset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commandId, command, status, offset);
  }
}
