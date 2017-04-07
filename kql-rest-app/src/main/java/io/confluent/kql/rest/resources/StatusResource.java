package io.confluent.kql.rest.resources;

import io.confluent.kql.rest.computation.StatementStatus;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@Path("/status")
@Produces(MediaType.APPLICATION_JSON)
public class StatusResource {

  private final Map<String, StatementStatus> statusStore;

  public StatusResource(Map<String, StatementStatus> statusStore) {
    this.statusStore = statusStore;
  }

  @GET
  public Response getAllStatuses() {
    try {
      JsonObjectBuilder result = Json.createObjectBuilder();
      JsonObjectBuilder statuses = Json.createObjectBuilder();

      for (Map.Entry<String, StatementStatus> queryStatus : statusStore.entrySet()) {
        statuses.add(queryStatus.getKey().toUpperCase(), queryStatus.getValue().getStatus().name());
      }

      return Response.ok(result.add("statuses", statuses.build()).build().toString()).build();
    } catch (Exception exception) {
      return KQLErrorResponse.stackTraceResponse(exception);
    }
  }

  @GET
  @Path("/{statementId}")
  public Response getQueryStatus(@PathParam("statementId") String statementId) {
    try {
      StatementStatus statementStatus = statusStore.get(statementId.toUpperCase());

      if (statementStatus == null) {
        throw new Exception(String.format("Statement not found: '%s", statementId));
      }

      JsonObjectBuilder status = Json.createObjectBuilder();
      status.add("statement_id", statementId.toUpperCase());
      status.add("status", statementStatus.getStatus().name());
      status.add("message", statementStatus.getMessage());

      JsonObjectBuilder result = Json.createObjectBuilder();
      return Response.ok(result.add("status", status.build()).build().toString()).build();
    } catch (Exception exception) {
      return KQLErrorResponse.stackTraceResponse(exception);
    }
  }
}
