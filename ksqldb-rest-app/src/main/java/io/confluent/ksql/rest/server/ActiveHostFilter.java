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
import io.confluent.ksql.util.KsqlHostInfo;
import org.apache.kafka.streams.state.HostInfo;

/**
 * Filters for the active host.
 */
public class ActiveHostFilter implements RoutingFilter {

  private final HostInfo activeHost;

  public ActiveHostFilter(final HostInfo activeHost) {
    this.activeHost = activeHost;
  }

  /**
   * Returns true if the host is the active host for a particular state store.
   * @param host The host for which the status is checked
   * @return true if the host is alive, false otherwise.
   */
  @Override
  public Host filter(final KsqlHostInfo host) {
    if (host.host().equals(activeHost.host()) && host.port() == activeHost.port()) {
      return Host.include(host);
    }
    return Host.exclude(host, "Host is not the active host for this partition.");
  }
}
