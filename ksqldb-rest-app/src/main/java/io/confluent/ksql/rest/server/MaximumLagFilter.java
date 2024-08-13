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

import com.google.common.base.Preconditions;
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

  /**
   * Creates a MaximumLagFilter
   * @param routingOptions The routing options used for filtering
   * @param lagByHost The map of hosts with the store to their lags
   * @param maxEndOffset The maximum end offset across hosts.
   */
  private MaximumLagFilter(
      final RoutingOptions routingOptions,
      final ImmutableMap<KsqlHostInfo, Optional<LagInfoEntity>> lagByHost,
      final OptionalLong maxEndOffset
  ) {
    this.lagByHost = requireNonNull(lagByHost, "lagByHost");
    this.routingOptions = requireNonNull(routingOptions, "routingOptions");
    this.maxEndOffset = requireNonNull(maxEndOffset, "maxEndOffset");
  }

  @Override
  public Host filter(final KsqlHostInfo hostInfo) {
    final long allowedOffsetLag = routingOptions.getMaxOffsetLagAllowed();
    final Optional<LagInfoEntity> lagInfoEntity = lagByHost.get(hostInfo);

    if (lagInfoEntity == null || !lagInfoEntity.isPresent()) {
      // If we don't have lag info, we'll be conservative and not include the host.  We have a
      // dual purpose, in both having HA and also having lag guarantees.  This ensures that we are
      // honoring the lag guarantees, and we'll try to minimize the window where lag isn't
      // available to promote HA.
      return Host.exclude(hostInfo, "Lag information is not present for host.");
    }

    final LagInfoEntity hostLag = lagInfoEntity.get();
    Preconditions.checkState(maxEndOffset.isPresent(), "Should have a maxEndOffset");
    // Compute the lag from the maximum end offset reported by all hosts.  This is so that
    // hosts that have fallen behind are held to the same end offset when computing lag.
    final long endOffset = maxEndOffset.getAsLong();
    final long offsetLag = Math.max(endOffset - hostLag.getCurrentOffsetPosition(), 0);
    if (offsetLag <= allowedOffsetLag) {
      return Host.include(hostInfo);
    } else {
      return Host.exclude(
          hostInfo,
          String.format(
              "Host excluded because lag %s exceeds maximum allowed lag %s.",
              offsetLag,
              allowedOffsetLag)
      );
    }
  }

  /**
   * Creates a FreshnessFilter
   * @param lagReportingAgent The optional lag reporting agent.
   * @param routingOptions The routing options
   * @param hosts The set of all hosts that have the store, including actives and standbys
   * @param queryId The query id of the persistent query that materialized the table
   * @param storeName The state store name of the materialized table
   * @param partition The partition of the topic
   * @return a new FreshnessFilter, unless lag reporting is disabled.
   */
  public static Optional<MaximumLagFilter> create(
      final Optional<LagReportingAgent> lagReportingAgent,
      final RoutingOptions routingOptions,
      final List<KsqlHostInfo> hosts,
      final String queryId,
      final String storeName,
      final int partition
  ) {
    if (!lagReportingAgent.isPresent()) {
      return Optional.empty();
    }
    final QueryStateStoreId queryStateStoreId = QueryStateStoreId.of(queryId, storeName);
    final ImmutableMap<KsqlHostInfo, Optional<LagInfoEntity>> lagByHost = hosts.stream()
        .collect(ImmutableMap.toImmutableMap(
            Function.identity(),
            host -> lagReportingAgent.get().getLagInfoForHost(
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
