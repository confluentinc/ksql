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

package io.confluent.ksql.rest;

import io.confluent.ksql.util.ErrorMessageUtil;

public class DefaultErrorMessages implements ErrorMessages {

  public static final String COMMAND_RUNNER_DEGRADED_INCOMPATIBLE_COMMANDS_ERROR_MESSAGE =
      "The server has encountered an incompatible entry in its log "
          + "and cannot process further DDL statements."
          + System.lineSeparator()
          + "This is most likely due to the service being rolled back to an earlier version.";

  public static final String COMMAND_RUNNER_DEGRADED_BACKUP_CORRUPTED_ERROR_MESSAGE =
      "The server has detected that the command topic may be corrupted. The backup of the "
          + "command topic does not match the current contents of command topic."
          + System.lineSeparator()
          + "DDL statements will not be processed until either:"
          + System.lineSeparator()
          + "1. The current command topic is deleted and the backup file is used "
          + "to restore the command topic."
          + System.lineSeparator()
          + "2. The current backup file is deleted."
          + System.lineSeparator()
          + "The server must be restarted after performing either operation in order to resume "
          + "normal functionality";
  

  @Override
  public String kafkaAuthorizationErrorMessage(final Exception e) {
    return ErrorMessageUtil.buildErrorMessage(e);
  }

  @Override
  public String transactionInitTimeoutErrorMessage(final Exception e) {
    return "Timeout while initializing transaction to the KSQL command topic."
        + System.lineSeparator()
        + "If you're running a single Kafka broker, " 
        + "ensure that the following configs are set to 1 on the broker:"
        + System.lineSeparator()
        + "- transaction.state.log.replication.factor"
        + System.lineSeparator()
        + "- transaction.state.log.min.isr"
        + System.lineSeparator()
        + "- offsets.topic.replication.factor";
  }

  @Override
  public String schemaRegistryUnconfiguredErrorMessage(final Exception e) {
    return ErrorMessageUtil.buildErrorMessage(e);
  }

  @Override
  public String commandRunnerDegradedIncompatibleCommandsErrorMessage() {
    return COMMAND_RUNNER_DEGRADED_INCOMPATIBLE_COMMANDS_ERROR_MESSAGE;
  }

  @Override
  public String commandRunnerDegradedBackupCorruptedErrorMessage() {
    return COMMAND_RUNNER_DEGRADED_BACKUP_CORRUPTED_ERROR_MESSAGE;
  }
}
