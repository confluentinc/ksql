/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.StatementExecutor;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Optional;

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
  @Path("/{type}/{entity}")
  public Response getStatus(@PathParam("type") String type, @PathParam("entity") String entity)
      throws Exception {
    CommandId commandId = new CommandId(type, entity);

    Optional<CommandStatus> commandStatus = statementExecutor.getStatus(commandId);

    if (!commandStatus.isPresent()) {
      throw new Exception("Command not found");
    }

    return Response.ok(commandStatus.get()).build();
  }
}
