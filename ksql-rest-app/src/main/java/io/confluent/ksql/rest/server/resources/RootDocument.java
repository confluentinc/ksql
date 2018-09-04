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
import java.net.URI;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

@Path("/")
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class RootDocument {

  private final String postFix;

  public RootDocument() {
    this.postFix = "info";
  }

  @GET
  public Response get(@Context final UriInfo uriInfo) {
    final URI uri = UriBuilder.fromUri(uriInfo.getAbsolutePath())
        .path(postFix)
        .build();

    return Response.temporaryRedirect(uri).build();
  }
}
