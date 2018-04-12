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

package io.confluent.ksql.rest.server.resources;

import java.util.Optional;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.StatementExecutor;

@Path("/status")
@Produces(MediaType.APPLICATION_JSON)
public class StatusResource {

  private final StatementExecutor statementExecutor;

  public StatusResource(StatementExecutor statementExecutor) {
    this.statementExecutor = statementExecutor;
  }

  @GET
  public Response getAllStatuses() {
    return Response.ok(CommandStatuses.fromFullStatuses(statementExecutor.getStatuses())).build();
  }

  @GET
  @Path("/{type}/{entity}/{action}")
  public Response getStatus(
      @PathParam("type") String type,
      @PathParam("entity") String entity,
      @PathParam("action") String action
  ) throws Exception {
    CommandId commandId = new CommandId(type, entity, action);

    Optional<CommandStatus> commandStatus = statementExecutor.getStatus(commandId);

    if (!commandStatus.isPresent()) {
      return Errors.notFound("Command not found");
    }

    return Response.ok(commandStatus.get()).build();
  }
}
