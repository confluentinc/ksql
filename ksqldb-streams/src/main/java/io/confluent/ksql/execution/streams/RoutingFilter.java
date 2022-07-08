/*o
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

package io.confluent.ksql.execution.streams;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.util.KsqlHostInfo;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.streams.state.HostInfo;

/**
 * Used to filter ksql hosts based on criteria specified in implementing classes.
 * One such example is a filter that checks whether hosts are alive or dead as determined
 * by the heartbeat agent.
 */
public interface RoutingFilter {

  /**
   * If the given host should be included for consideration.
   * @param hostInfo The host to be considered for this filter
   * @return If the host should be included for consideration.
   */
  Host filter(KsqlHostInfo hostInfo);

  @Immutable
  class Host {
    private final KsqlHostInfo sourceInfo;
    private final boolean selected;
    private final String reasonNotSelected;

    private Host(
        final KsqlHostInfo sourceInfo,
        final boolean selected,
        final String reasonNotSelected
    ) {
      this.sourceInfo = Objects.requireNonNull(sourceInfo, "sourceInfo");
      this.selected = selected;
      this.reasonNotSelected = Objects.requireNonNull(reasonNotSelected, "reasonNotSelected");
    }

    public static Host include(final KsqlHostInfo host) {
      return new Host(host, true, "");
    }

    public static Host exclude(final KsqlHostInfo host, final String reason) {
      return new Host(host, false, reason);
    }

    public KsqlHostInfo getSourceInfo() {
      return sourceInfo;
    }

    public boolean isSelected() {
      return selected;
    }

    public String getReasonNotSelected() {
      return reasonNotSelected;
    }

    public Host combine(final Host other) {
      if (!sourceInfo.equals(other.sourceInfo)) {
        throw new IllegalStateException("Should not combine non-matching hosts");
      }

      return new Host(
          sourceInfo,
          selected && other.selected,
          Stream.of(reasonNotSelected, other.reasonNotSelected)
              .filter(s -> !s.isEmpty()).collect(Collectors.joining(","))
      );
    }

    @Override
    public String toString() {
      return sourceInfo + " was "
          + (selected ? "selected" : "not selected because " + reasonNotSelected);
    }
  }

  /**
   * A factory for RoutingFilters.
   */
  interface RoutingFilterFactory {

    /**
     * Creates a RoutingFilter
     * @param routingOptions The options to use when filtering
     * @param hosts The set of all hosts that have the store, including actives and standbys
     * @param activeHost The active host
     * @param applicationQueryId The query id of the persistent query that materialized the table
     * @param storeName The state store name of the materialized table
     * @param partition The partition of the changelog topic
     * @return The new RoutingFilter
     */
    RoutingFilter createRoutingFilter(
        RoutingOptions routingOptions,
        List<KsqlHostInfo> hosts,
        HostInfo activeHost,
        String applicationQueryId,
        String storeName,
        int partition);
  }

}
