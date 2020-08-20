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

  static String COMMAND_RUNNER_DEGRADED_ERROR_MESSAGE =
      "The server has encountered an incompatible entry in its log "
          + "and cannot process further DDL statements."
          + System.lineSeparator()
          + "This is most likely due to the service being rolled back to an earlier version.";

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
  public String commandRunnerDegradedErrorMessage() {
    return COMMAND_RUNNER_DEGRADED_ERROR_MESSAGE;
  }
}
