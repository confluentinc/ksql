/*
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

import io.confluent.ksql.rest.entity.Versions;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/info")
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class ServerInfoResource {

  private final io.confluent.ksql.rest.entity.ServerInfo serverInfo;

  public ServerInfoResource(final io.confluent.ksql.rest.entity.ServerInfo serverInfo) {
    this.serverInfo = serverInfo;
  }

  @GET
  public Response get() {
    return Response.ok(serverInfo).build();
  }
}
