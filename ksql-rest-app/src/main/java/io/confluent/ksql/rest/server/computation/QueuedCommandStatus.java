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

import io.confluent.ksql.rest.entity.CommandStatus;

import java.time.Duration;
import java.util.Objects;

public class QueuedCommandStatus {
  private final CommandStatusFuture commandStatusFuture;
  private final long commandSequenceNumber;

  public QueuedCommandStatus(
      final long commandSequenceNumber, final CommandStatusFuture commandStatusFuture) {
    this.commandSequenceNumber =
        Objects.requireNonNull(commandSequenceNumber, "commandSequenceNumber");
    this.commandStatusFuture = Objects.requireNonNull(commandStatusFuture, "commandStatusFuture");
  }

  public CommandStatus getStatus() {
    return commandStatusFuture.getStatus();
  }

  public CommandId getCommandId() {
    return commandStatusFuture.getCommandId();
  }

  public long getCommandSequenceNumber() {
    return commandSequenceNumber;
  }

  public CommandStatus tryWaitForFinalStatus(final Duration timeout) throws InterruptedException {
    return commandStatusFuture.tryWaitForFinalStatus(timeout);
  }
}
