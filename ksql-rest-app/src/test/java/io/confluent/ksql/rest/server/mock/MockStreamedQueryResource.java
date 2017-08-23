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
