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

import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.server.computation.InteractiveStatementExecutor;
import java.util.Optional;
import javax.ws.rs.core.Response;

public class StatusResource {

  private final InteractiveStatementExecutor statementExecutor;

  public StatusResource(final InteractiveStatementExecutor statementExecutor) {
    this.statementExecutor = statementExecutor;
  }

  public Response getAllStatuses() {
    return Response.ok(CommandStatuses.fromFullStatuses(statementExecutor.getStatuses())).build();
  }

  public Response getStatus(final String type, final String entity, final String action) {
    final CommandId commandId = new CommandId(type, entity, action);

    final Optional<CommandStatus> commandStatus = statementExecutor.getStatus(commandId);

    if (!commandStatus.isPresent()) {
      return Errors.notFound("Command not found");
    }

    return Response.ok(commandStatus.get()).build();
  }
}
