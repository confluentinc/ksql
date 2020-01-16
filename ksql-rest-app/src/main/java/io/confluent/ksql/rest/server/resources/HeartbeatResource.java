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

import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.HeartbeatResponse;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.HeartbeatAgent;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.kafka.streams.state.HostInfo;

/**
 * Endpoint for registering heartbeats received from remote servers. The heartbeats are used
 * to determine the status of the remote servers, i.e. whether they are alive or dead.
 */

@Path("/heartbeat")
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class HeartbeatResource {

  private final HeartbeatAgent heartbeatAgent;

  public HeartbeatResource(final HeartbeatAgent heartbeatAgent) {
    this.heartbeatAgent = heartbeatAgent;
  }

  @POST
  public Response registerHeartbeat(final HeartbeatMessage request) {
    handleHeartbeat(request);
    return Response.ok(new HeartbeatResponse(true)).build();
  }

  private void handleHeartbeat(final HeartbeatMessage request) {
    final HostInfoEntity hostInfoEntity = request.getHostInfo();
    final HostInfo hostInfo = new HostInfo(hostInfoEntity.getHost(), hostInfoEntity.getPort());
    final long timestamp = request.getTimestamp();
    heartbeatAgent.receiveHeartbeat(hostInfo, timestamp);
  }
}
