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

package io.confluent.ksql.rest.server.resources;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.server.computation.InteractiveStatementExecutor;
import java.util.Optional;

public class StatusResource {

  private final InteractiveStatementExecutor statementExecutor;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public StatusResource(final InteractiveStatementExecutor statementExecutor) {
    this.statementExecutor = statementExecutor;
  }

  public EndpointResponse getAllStatuses() {
    return EndpointResponse.ok(CommandStatuses.fromFullStatuses(statementExecutor.getStatuses()));
  }

  public EndpointResponse getStatus(final String type, final String entity, final String action) {
    final CommandId commandId = new CommandId(type, entity, action);

    final Optional<CommandStatus> commandStatus = statementExecutor.getStatus(commandId);

    return commandStatus.map(EndpointResponse::ok)
        .orElseGet(() -> Errors.notFound("Command not found"));

  }
}
