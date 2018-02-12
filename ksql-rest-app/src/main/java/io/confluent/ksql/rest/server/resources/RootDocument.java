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

package io.confluent.ksql.rest.server.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class RootDocument {

  private final boolean uiEnabled;
  private final String uiUrl;
  private final String infoUrl;

  public RootDocument(boolean uiEnabled, String baseUrl) {
    this.uiEnabled = uiEnabled;
    this.uiUrl = baseUrl + "/index.html";
    this.infoUrl = baseUrl + "/info";
  }

  @GET
  public Response get() {
    try {
      if (uiEnabled) {
        return Response.temporaryRedirect(new URI(uiUrl)).build();
      } else {
        return Response.temporaryRedirect(new URI(infoUrl)).build();
      }
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return Response.serverError().build();
  }
}
