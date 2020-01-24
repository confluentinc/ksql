/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.HeartbeatAgent;
import io.confluent.ksql.rest.server.LagReportingAgent;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Endpoint that reports the view of the cluster that this server has.
 * Returns every host that has been discovered by this server along side with information about its
 * status such as whether it is alive or dead and the last time its status got updated.
 */

@Path("/clusterStatus")
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class ClusterStatusResource {

  private final HeartbeatAgent heartbeatAgent;
  private final Optional<LagReportingAgent> lagReportingAgent;

  public ClusterStatusResource(final HeartbeatAgent heartbeatAgent,
                               final Optional<LagReportingAgent> lagReportingAgent) {
    this.heartbeatAgent = heartbeatAgent;
    this.lagReportingAgent = lagReportingAgent;
  }

  @GET
  public Response checkClusterStatus() {
    final ClusterStatusResponse response = getResponse();
    return Response.ok(response).build();
  }

  private ClusterStatusResponse getResponse() {
    Map<HostInfoEntity, Map<QueryStateStoreId, Map<Integer, LagInfoEntity>>> lags =
        lagReportingAgent.isPresent() ?
            lagReportingAgent.get().listAllLags() :
            Collections.emptyMap();
    return new ClusterStatusResponse(heartbeatAgent.getHostsStatus(), lags);
  }
}
