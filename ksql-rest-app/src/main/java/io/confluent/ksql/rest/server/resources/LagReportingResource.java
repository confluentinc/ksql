/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.rest.entity.LagListResponse;
import io.confluent.ksql.rest.entity.LagReportingRequest;
import io.confluent.ksql.rest.entity.LagReportingResponse;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.LagReportingAgent;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/lag")
@Consumes({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
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
    return Response.ok(new LagListResponse(lagReportingAgent.listAllLags())).build();
  }

}
