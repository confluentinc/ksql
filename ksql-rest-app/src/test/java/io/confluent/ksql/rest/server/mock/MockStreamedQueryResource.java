package io.confluent.ksql.rest.server.mock;


import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import io.confluent.ksql.rest.entity.KsqlRequest;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
public class MockStreamedQueryResource {

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response streamQuery(KsqlRequest request) throws Exception {
    TestStreamWriter testStreamWriter = new TestStreamWriter();
    return Response.ok().entity(testStreamWriter).build();
  }

  private class TestStreamWriter implements StreamingOutput {

    @Override
    public void write(OutputStream out) throws IOException, WebApplicationException {
      synchronized (out) {
        out.write("Hello".getBytes());
        out.flush();
      }
    }
  }
}
