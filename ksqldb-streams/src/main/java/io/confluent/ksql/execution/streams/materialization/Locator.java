/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams.materialization;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.RoutingFilter;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Type used to locate on which KSQL node materialized data is stored.
 *
 * <p>Data stored in materialized stores can be spread across KSQL nodes. This type can be used to
 * determine which KSQL server stores a specific key.
 */
public interface Locator {

  /**
   * Locate which KSQL nodes store the supplied {@code key}.
   *
   * <p>Implementations are free to return {@link Optional#empty()} if the location is not known at
   * this time.
   *
   * @param keys the keys to locate. If none are provided, it's assumed that we should locate all
   *             partitions for the given state store.
   * @return the list of nodes, that can potentially serve the key.
   */
  List<KsqlPartitionLocation> locate(
      List<KsqlKey> keys,
      RoutingOptions routingOptions,
      RoutingFilterFactory routingFilterFactory,
      boolean isRangeScan
  );

  interface KsqlNode {

    /**
     * @return {@code true} if this is the local node, i.e. the KSQL instance handling the call.
     */
    boolean isLocal();

    /**
     * @return The base URI of the node, including protocol, host and port.
     */
    URI location();

    /**
     * @return the host as well as whether or not it should be queried
     */
    RoutingFilter.Host getHost();
  }

  interface KsqlPartitionLocation {

    /**
     * @return the ordered and filtered list of nodes to contact to access the above key.
     */
    List<KsqlNode> getNodes();

    /**
     * @return The partition associated with the given data we want to access.
     */
    int getPartition();

    /**
     * @return the keys associated with the data we want to access, if any. Keys may not be present
     *     for queries which don't enumerate them up front, such as range queries.
     */
    Optional<Set<KsqlKey>> getKeys();

    /**
     * @return a {@code KsqlPartitionLocation} without any hosts that were indicated by the
     *         {@link RoutingOptions} as invalid candidates for routing.
     */
    KsqlPartitionLocation removeFilteredHosts();

    /**
     * @return a {@code KsqlPartitionLocation} without the head node.
     */
    KsqlPartitionLocation removeHeadHost();
  }

  /**
   * Wrapper around a GenericKey
   */
  interface KsqlKey {

    /**
     * Gets the key associated with this KsqlKey
     */
    GenericKey getKey();
  }
}
