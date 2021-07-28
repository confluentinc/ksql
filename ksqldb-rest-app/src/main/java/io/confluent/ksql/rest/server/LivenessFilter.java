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

package io.confluent.ksql.rest.server;

import io.confluent.ksql.execution.streams.RoutingFilter;
import io.confluent.ksql.util.HostStatus;
import io.confluent.ksql.util.KsqlHostInfo;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Filters ksql hosts based on whether they are alive or dead.
 */
public class LivenessFilter implements RoutingFilter {

  private final Optional<HeartbeatAgent> heartbeatAgent;

  public LivenessFilter(final Optional<HeartbeatAgent> heartbeatAgent) {
    this.heartbeatAgent = Objects.requireNonNull(heartbeatAgent, "heartbeatAgent");
  }

  /**
   * Returns true if the host is alive. If the heartbeat agent is not enabled, all hosts are
   * assumed to be alive.
   * @param host The host for which the status is checked
   * @return true if the host is alive, false otherwise.
   */
  @Override
  public Host filter(final KsqlHostInfo host) {

    if (!heartbeatAgent.isPresent()) {
      return Host.include(host);
    }

    final Map<KsqlHostInfo, HostStatus> allHostsStatus = heartbeatAgent.get().getHostsStatus();
    final HostStatus status = allHostsStatus.get(host);
    if (status == null) {
      return Host.include(host);
    }

    if (status.isHostAlive()) {
      return Host.include(host);
    } else {
      return Host.exclude(host, "Host is not alive as of time " + status.getLastStatusUpdateMs());
    }
  }
}
