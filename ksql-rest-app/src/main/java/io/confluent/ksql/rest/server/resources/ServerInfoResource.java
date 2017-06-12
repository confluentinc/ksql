/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.rest.entity.ServerInfo;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class ServerInfoResource {

  private final ServerInfo serverInfo;

  public ServerInfoResource(ServerInfo serverInfo) {
    this.serverInfo = serverInfo;
  }

  @GET
  public Response getServerInfo() {
    return Response.ok(serverInfo).build();
  }
}
