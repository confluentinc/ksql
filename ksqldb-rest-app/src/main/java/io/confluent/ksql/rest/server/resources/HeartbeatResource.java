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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.HeartbeatResponse;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.server.HeartbeatAgent;
import io.confluent.ksql.util.KsqlHostInfo;

/**
 * Endpoint for registering heartbeats received from remote servers. The heartbeats are used
 * to determine the status of the remote servers, i.e. whether they are alive or dead.
 */

public class HeartbeatResource {

  private final HeartbeatAgent heartbeatAgent;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public HeartbeatResource(final HeartbeatAgent heartbeatAgent) {
    this.heartbeatAgent = heartbeatAgent;
  }

  public EndpointResponse registerHeartbeat(final HeartbeatMessage request) {
    handleHeartbeat(request);
    return EndpointResponse.ok(new HeartbeatResponse(true));
  }

  private void handleHeartbeat(final HeartbeatMessage request) {
    final KsqlHostInfoEntity ksqlHostInfoEntity = request.getHostInfo();
    final KsqlHostInfo ksqlHostInfo = new KsqlHostInfo(
        ksqlHostInfoEntity.getHost(), ksqlHostInfoEntity.getPort());
    final long timestamp = request.getTimestamp();
    heartbeatAgent.receiveHeartbeat(ksqlHostInfo, timestamp);
  }
}
