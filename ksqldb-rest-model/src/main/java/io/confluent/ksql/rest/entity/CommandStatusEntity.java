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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CommandStatusEntity extends KsqlEntity {
  private final CommandId commandId;
  private final CommandStatus commandStatus;
  private final long commandSequenceNumber;

  public CommandStatusEntity(
      final String statementText,
      final CommandId commandId,
      final CommandStatus commandStatus,
      final Long commandSequenceNumber
  ) {
    this(statementText, commandId, commandStatus, commandSequenceNumber, Collections.emptyList());
  }

  @JsonCreator
  public CommandStatusEntity(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("commandId") final CommandId commandId,
      @JsonProperty("commandStatus") final CommandStatus commandStatus,
      @JsonProperty("commandSequenceNumber") final Long commandSequenceNumber,
      @JsonProperty("warnings") final List<KsqlWarning> warnings
  ) {
    super(statementText, warnings);
    this.commandId = Objects.requireNonNull(commandId, "commandId");
    this.commandStatus = Objects.requireNonNull(commandStatus, "commandStatus");
    this.commandSequenceNumber = commandSequenceNumber == null ? -1 : commandSequenceNumber;
  }

  public CommandId getCommandId() {
    return commandId;
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
  public CommandStatus getCommandStatus() {
    return commandStatus;
  }

  public long getCommandSequenceNumber() {
    return commandSequenceNumber;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CommandStatusEntity)) {
      return false;
    }
    final CommandStatusEntity that = (CommandStatusEntity) o;
    return Objects.equals(commandId, that.commandId)
        && Objects.equals(commandStatus, that.commandStatus)
        && (commandSequenceNumber == that.commandSequenceNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commandId, commandStatus, commandSequenceNumber);
  }

  @Override
  public String toString() {
    return "CommandStatusEntity{"
        + "commandId=" + commandId
        + ", commandStatus=" + commandStatus
        + ", commandSequenceNumber=" + commandSequenceNumber
        + '}';
  }
}
