/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.streams.RoutingFilter;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.util.KsqlHostInfo;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

/**
 * A RoutingFilter that filters hosts based upon changelog processing lag.
 */
public final class MaximumLagFilter implements RoutingFilter {

  private final ImmutableMap<KsqlHostInfo, Optional<LagInfoEntity>> lagByHost;
  private final RoutingOptions routingOptions;
  private final OptionalLong maxEndOffset;

  private MaximumLagFilter(
      final RoutingOptions routingOptions,
      final ImmutableMap<KsqlHostInfo, Optional<LagInfoEntity>> lagByHost,
      final OptionalLong maxEndOffset
  ) {
    this.lagByHost = requireNonNull(lagByHost, "lagByHost");
    this.routingOptions = requireNonNull(routingOptions, "requireNonNull");
    this.maxEndOffset = requireNonNull(maxEndOffset, "maxEndOffset");
  }

  @Override
  public boolean filter(final KsqlHostInfo hostInfo) {
    final long allowedOffsetLag = routingOptions.getOffsetLagAllowed();
    if (allowedOffsetLag >= 0) {
      return lagByHost.getOrDefault(hostInfo, Optional.empty())
          .map(hostLag -> {
            // Compute the lag from the maximum end offset we've seen
            final long endOffset = maxEndOffset.orElse(hostLag.getEndOffsetPosition());
            final long offsetLag = Math.max(endOffset - hostLag.getCurrentOffsetPosition(), 0);
            return offsetLag <= allowedOffsetLag;
          })
          // If we don't have lag info, we'll be conservative and include the host
          .orElse(true);
    }
    return true;
  }

  /**
   * Creates a FreshnessFilter
   * @param lagReportingAgent The optional lag reporting agent.
   * @param routingOptions The routing options
   * @param hosts The set of all hosts that have the store, including actives and standbys
   * @param applicationQueryId The application query id
   * @param storeName The state store name
   * @param partition The partition of the topic
   * @return a new FreshnessFilter, unless lag reporting is disabled.
   */
  public static Optional<MaximumLagFilter> create(
      final Optional<LagReportingAgent> lagReportingAgent,
      final RoutingOptions routingOptions,
      final List<KsqlHostInfo> hosts,
      final String applicationQueryId,
      final String storeName,
      final int partition
  ) {
    if (!lagReportingAgent.isPresent()) {
      return Optional.empty();
    }
    final QueryStateStoreId queryStateStoreId = QueryStateStoreId.of(applicationQueryId, storeName);
    final ImmutableMap<KsqlHostInfo, Optional<LagInfoEntity>> lagByHost = hosts.stream()
        .collect(ImmutableMap.toImmutableMap(
            Function.identity(),
            host -> lagReportingAgent.get().getHostsPartitionLagInfo(
                host,
                queryStateStoreId,
                partition)));
    final OptionalLong maxEndOffset = lagByHost.values().stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .mapToLong(LagInfoEntity::getEndOffsetPosition)
        .max();
    return Optional.of(new MaximumLagFilter(routingOptions, lagByHost, maxEndOffset));
  }
}
