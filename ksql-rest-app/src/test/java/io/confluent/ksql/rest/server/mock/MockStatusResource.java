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

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.server.computation.CommandId;

@Path("/status")
@Produces(MediaType.APPLICATION_JSON)
public class MockStatusResource {

  @GET
  public Response getAllStatuses() {
    Map<CommandId, CommandStatus.Status> statuses = new
        HashMap<>();
    statuses.put(new CommandId(CommandId.Type.TOPIC, "c1"), CommandStatus.Status.SUCCESS);
    statuses.put(new CommandId(CommandId.Type.TOPIC, "c2"), CommandStatus.Status.ERROR);
    CommandStatuses commandStatuses = new CommandStatuses(statuses);
    return Response.ok(commandStatuses).build();
  }

  @GET
  @Path("/{type}/{entity}")
  public Response getStatus(@PathParam("type") String type, @PathParam("entity") String entity)
      throws Exception {
    return Response.ok("status").build();
  }
}
