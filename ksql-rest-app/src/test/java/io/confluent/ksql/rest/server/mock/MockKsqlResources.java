package io.confluent.ksql.rest.server.mock;


import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;

@Path("/ksql")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MockKsqlResources {


  @POST
  public Response handleKsqlStatements(KsqlRequest request) throws Exception {

    KsqlEntityList result = new KsqlEntityList();
    result.add(new ExecutionPlan("TestExecution plan"));
    return Response.ok(result).build();
  }

}
