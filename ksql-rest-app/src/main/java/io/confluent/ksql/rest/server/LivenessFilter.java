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
import io.confluent.ksql.util.KsqlHost;
import java.util.Map;
import org.apache.kafka.streams.state.HostInfo;

/**
 * Filters ksql hosts based on whether they are alive or dead.
 */
public class LivenessFilter implements RoutingFilter {

  public LivenessFilter() {
  }

  /**
   * Returns true if the host is alive. If the heartbeat agent is not enabled, all hosts are
   * assumed to be alive.
   * @param allHostsStatus status of all hosts as provided by heartbeat agent
   * @param activeHost the active host for a particular state store
   * @param host The host for which the status is checked
   * @param storeName Ignored
   * @param partition Ignored
   * @return true if the host is alive, false otherwise.
   */
  @Override
  public boolean filter(
      final Map<KsqlHost, HostStatus> allHostsStatus,
      final HostInfo activeHost,
      final KsqlHost host,
      final String storeName,
      final int partition) {

    final HostStatus status = allHostsStatus.get(host);
    return status == null ? true : allHostsStatus.get(host).isHostAlive();
  }
}
