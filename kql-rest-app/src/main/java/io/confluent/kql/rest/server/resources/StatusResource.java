package io.confluent.kql.rest.server.resources;

import io.confluent.kql.rest.server.computation.StatementExecutor;
import io.confluent.kql.rest.server.computation.StatementStatus;

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

    for (Map.Entry<String, StatementStatus> queryStatus : statementExecutor.getStatuses().entrySet()) {
      statuses.add(queryStatus.getKey().toUpperCase(), queryStatus.getValue().getStatus().name());
    }

    return Response.ok(result.add("statuses", statuses.build()).build().toString()).build();
  }

  @GET
  @Path("/{statementId}")
  public Response getQueryStatus(@PathParam("statementId") String statementId) throws Exception {
    Optional<StatementStatus> statementStatus = statementExecutor.getStatus(statementId.toUpperCase());

    if (!statementStatus.isPresent()) {
      throw new Exception(String.format("Statement not found: '%s", statementId));
    }

    JsonObjectBuilder status = Json.createObjectBuilder();
    status.add("statement_id", statementId.toUpperCase());
    status.add("status", statementStatus.get().getStatus().name());
    status.add("message", statementStatus.get().getMessage());

    JsonObjectBuilder result = Json.createObjectBuilder();
    return Response.ok(result.add("status", status.build()).build().toString()).build();
  }
}
