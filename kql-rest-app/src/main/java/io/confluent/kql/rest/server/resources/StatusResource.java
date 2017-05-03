/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest.server.resources;

import io.confluent.kql.rest.server.computation.CommandId;
import io.confluent.kql.rest.server.computation.StatementExecutor;
import io.confluent.kql.rest.server.computation.CommandStatus;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
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
    JsonObjectBuilder result = Json.createObjectBuilder();
    JsonObjectBuilder statuses = Json.createObjectBuilder();

    for (Map.Entry<CommandId, CommandStatus> queryStatus : statementExecutor.getStatuses().entrySet()) {
      statuses.add(queryStatus.getKey().toString(), queryStatus.getValue().getStatus().name());
    }

    return Response.ok(result.add("statuses", statuses.build()).build().toString()).build();
  }

  @GET
  @Path("/{type}/{entity}")
  public Response getQueryStatus(@PathParam("type") String type, @PathParam("entity") String entity) throws Exception {
    CommandId commandId = new CommandId(type, entity);

    Optional<CommandStatus> statementStatus = statementExecutor.getStatus(commandId);

    if (!statementStatus.isPresent()) {
      throw new Exception("Command not found");
    }

    JsonObjectBuilder status = Json.createObjectBuilder();
    status.add("command_id", commandId.toString());
    status.add("status", statementStatus.get().getStatus().name());
    status.add("message", statementStatus.get().getMessage());

    JsonObjectBuilder result = Json.createObjectBuilder();
    return Response.ok(result.add("status", status.build()).build().toString()).build();
  }
}
