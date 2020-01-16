package io.confluent.ksql.rest.server.services;

import io.confluent.ksql.rest.entity.LagReportingRequest;
import io.confluent.ksql.rest.entity.LagReportingResponse;
import io.confluent.ksql.rest.entity.LagListResponse;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.LagReportingAgent;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/lag")
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class LagReportingResource {

  private LagReportingAgent lagReportingAgent;

  public LagReportingResource(final LagReportingAgent lagReportingAgent) {
    this.lagReportingAgent = lagReportingAgent;
  }

  @Path("/report")
  @POST
  public Response receiveHostLag(final LagReportingRequest request) {
    lagReportingAgent.receiveHostLag(request);
    return Response.ok(new LagReportingResponse(true)).build();
  }

  @Path("/list")
  @GET
  public Response listLags() {
    return Response.ok(new LagListResponse(lagReportingAgent.listAllCurrentPositions())).build();
  }

}
