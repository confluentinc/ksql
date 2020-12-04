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

package io.confluent.ksql.execution.streams.materialization.ks;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.streams.processor.internals.StreamsMetadataState.UNKNOWN_HOST;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.RoutingFilter;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.execution.streams.materialization.Locator;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.util.KsqlHostInfo;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams implementation of {@link Locator}.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public final class KsLocator implements Locator {

  private static final Logger LOG = LoggerFactory.getLogger(KsLocator.class);
  private final String stateStoreName;
  private final KafkaStreams kafkaStreams;
  private final Serializer<GenericKey> keySerializer;
  private final URL localHost;
  private final String applicationId;

  KsLocator(
      final String stateStoreName,
      final KafkaStreams kafkaStreams,
      final Serializer<GenericKey> keySerializer,
      final URL localHost,
      final String applicationId
  ) {
    this.kafkaStreams = requireNonNull(kafkaStreams, "kafkaStreams");
    this.keySerializer = requireNonNull(keySerializer, "keySerializer");
    this.stateStoreName = requireNonNull(stateStoreName, "stateStoreName");
    this.localHost = requireNonNull(localHost, "localHost");
    this.applicationId = requireNonNull(applicationId, "applicationId");;
  }

  @Override
  public List<KsqlPartitionLocation> locate(
      final List<GenericKey> keys,
      final RoutingOptions routingOptions,
      final RoutingFilterFactory routingFilterFactory
  ) {
    // Maintain request order for reproducibility by using a LinkedHashMap, even though it's
    // not a guarantee of the API.
    final Map<Integer, List<KsqlNode>> locationsByPartition = new LinkedHashMap<>();
    final Set<Integer> filterPartitions = routingOptions.getPartitions();

    final Metadata metadata = keys.isEmpty()
        ? getMetadataForAllPartitions(filterPartitions)
        : getMetadataForKeys(keys, filterPartitions);
    for (KeyQueryMetadata keyQueryMetadata : metadata.getMetadataList()) {
      LOG.debug("Handling pull query for partition {} of state store {}.",
          keyQueryMetadata.partition(), stateStoreName);
      final HostInfo activeHost = keyQueryMetadata.activeHost();
      final Set<HostInfo> standByHosts = keyQueryMetadata.standbyHosts();

      if (locationsByPartition.containsKey(keyQueryMetadata.partition())) {
        continue;
      }

      final List<KsqlNode> filteredHosts = getFilteredHosts(routingOptions, routingFilterFactory,
          activeHost, standByHosts, keyQueryMetadata.partition());

      locationsByPartition.put(keyQueryMetadata.partition(), filteredHosts);
    }
    return locationsByPartition.entrySet().stream()
        .map(e -> new PartitionLocation(
            metadata.getKeysByPartition().map(keysByPartition -> keysByPartition.get(e.getKey())),
            e.getKey(), e.getValue()))
        .collect(ImmutableList.toImmutableList());
  }

  private Metadata getMetadataForKeys(
      final List<GenericKey> keys,
      final Set<Integer> filterPartitions
  ) {
    final Map<Integer, Set<GenericKey>> keysByPartition = new HashMap<>();
    final List<KeyQueryMetadata> metadataList = new ArrayList<>();
    for (GenericKey key : keys) {
      final KeyQueryMetadata metadata = kafkaStreams
          .queryMetadataForKey(stateStoreName, key, keySerializer);

      // Fail fast if Streams not ready. Let client handle it
      if (metadata == KeyQueryMetadata.NOT_AVAILABLE) {
        LOG.debug("KeyQueryMetadata not available for state store '{}' and key {}",
            stateStoreName, key);
        throw new MaterializationException(String.format(
            "Materialized data for key %s is not available yet. "
                + "Please try again later.", key));
      }

      LOG.debug("Handling pull query for key {} in partition {} of state store {}.",
          key, metadata.partition(), stateStoreName);
      final HostInfo activeHost = metadata.activeHost();
      final Set<HostInfo> standByHosts = metadata.standbyHosts();

      if (filterPartitions.size() > 0 && !filterPartitions.contains(metadata.partition())) {
        LOG.debug("Ignoring key {} in partition {} because parition is not included in lookup.",
            key, metadata.partition());
        continue;
      }

      keysByPartition.putIfAbsent(metadata.partition(), new LinkedHashSet<>());
      keysByPartition.get(metadata.partition()).add(key);
      metadataList.add(metadata);
    }
    return new Metadata(metadataList, Optional.of(keysByPartition));
  }

  private Metadata getMetadataForAllPartitions(final Set<Integer> filterPartitions) {
    final Map<Integer, HostInfo> activeHostByPartition = new HashMap<>();
    final Map<Integer, Set<HostInfo>> standbyHostsByPartition = new HashMap<>();
    for (final StreamsMetadata streamsMetadata : kafkaStreams.allMetadataForStore(stateStoreName)) {
      streamsMetadata.topicPartitions().forEach(
          tp -> activeHostByPartition.put(tp.partition(), streamsMetadata.hostInfo()));

      streamsMetadata.standbyTopicPartitions().forEach(
          tp -> {
            standbyHostsByPartition.computeIfAbsent(tp.partition(), p -> new HashSet<>());
            standbyHostsByPartition.get(tp.partition()).add(streamsMetadata.hostInfo());
          });
    }

    final Set<Integer> partitions = Streams.concat(
        activeHostByPartition.keySet().stream(),
        standbyHostsByPartition.keySet().stream())
        .collect(Collectors.toSet());

    final List<KeyQueryMetadata> metadataList = new ArrayList<>();
    for (Integer partition : partitions) {
      if (filterPartitions.size() > 0 && !filterPartitions.contains(partition)) {
        LOG.debug("Ignoring partition {} because partition is not included in lookup.", partition);
        continue;
      }
      final HostInfo activeHost = activeHostByPartition.getOrDefault(partition, UNKNOWN_HOST);
      final Set<HostInfo> standbyHosts = standbyHostsByPartition.getOrDefault(partition,
          Collections.emptySet());
      metadataList.add(new KeyQueryMetadata(activeHost, standbyHosts, partition));
    }
    return new Metadata(metadataList, Optional.empty());
  }

  private List<KsqlNode> getFilteredHosts(
      final RoutingOptions routingOptions,
      final RoutingFilterFactory routingFilterFactory,
      final HostInfo activeHost,
      final Set<HostInfo> standByHosts,
      final int partition
  ) {
    // If the lookup is for a forwarded request, only filter localhost
    List<KsqlHostInfo> allHosts = null;
    if (routingOptions.getIsSkipForwardRequest()) {
      LOG.debug("Before filtering: Local host {} ", localHost);
      allHosts = ImmutableList.of(new KsqlHostInfo(localHost.getHost(), localHost.getPort()));
    } else {
      LOG.debug("Before filtering: Active host {} , standby hosts {}", activeHost, standByHosts);
      allHosts = Stream.concat(Stream.of(activeHost), standByHosts.stream())
          .map(this::asKsqlHost)
          .collect(Collectors.toList());
    }
    final RoutingFilter routingFilter = routingFilterFactory.createRoutingFilter(routingOptions,
        allHosts, activeHost, applicationId, stateStoreName, partition);

    // Filter out hosts based on active, liveness and max lag filters.
    // The list is ordered by routing preference: active node is first, then standby nodes.
    // If heartbeat is not enabled, all hosts are considered alive.
    // If the request is forwarded internally from another ksql server, only the max lag filter
    // is applied.
    final ImmutableList<KsqlNode> filteredHosts = allHosts.stream()
        .filter(routingFilter::filter)
        .map(this::asNode)
        .collect(ImmutableList.toImmutableList());

    LOG.debug("Filtered and ordered hosts: {}", filteredHosts);

    return filteredHosts;
  }

  @VisibleForTesting
  KsqlHostInfo asKsqlHost(final HostInfo hostInfo) {
    return new KsqlHostInfo(hostInfo.host(), hostInfo.port());
  }

  @VisibleForTesting
  KsqlNode asNode(final KsqlHostInfo host) {
    return new Node(
        isLocalHost(host),
        buildLocation(host)
    );
  }

  private boolean isLocalHost(final KsqlHostInfo hostInfo) {
    if (hostInfo.port() != localHost.getPort()) {
      return false;
    }

    return hostInfo.host().equalsIgnoreCase(localHost.getHost())
        || hostInfo.host().equalsIgnoreCase("localhost");
  }

  private URI buildLocation(final KsqlHostInfo remoteInfo) {
    try {
      return new URL(
          localHost.getProtocol(),
          remoteInfo.host(),
          remoteInfo.port(),
          "/"
      ).toURI();
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to convert remote host info to URL."
          + " remoteInfo: " + remoteInfo);
    }
  }

  @Immutable
  @VisibleForTesting
  public static final class Node implements KsqlNode {

    private final boolean local;
    private final URI location;

    private Node(final boolean local, final URI location) {
      this.local = local;
      this.location = requireNonNull(location, "location");
    }

    @Override
    public boolean isLocal() {
      return local;
    }

    @Override
    public URI location() {
      return location;
    }

    @Override
    public String toString() {
      return "Node{"
          + "local = " + local
          + ", location = " + location
          + "}";
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Node that = (Node) o;
      return local == that.local
          && location.equals(that.location);
    }

    @Override
    public int hashCode() {
      return Objects.hash(local, location);
    }

  }

  @VisibleForTesting
  public static final class PartitionLocation implements KsqlPartitionLocation {
    private final Optional<Set<GenericKey>> keys;
    private final int partition;
    private final List<KsqlNode> nodes;

    public PartitionLocation(final Optional<Set<GenericKey>> keys, final int partition,
                             final List<KsqlNode> nodes) {
      this.keys = keys;
      this.partition = partition;
      this.nodes = nodes;
    }

    public Optional<Set<GenericKey>> getKeys() {
      return keys;
    }

    public List<KsqlNode> getNodes() {
      return nodes;
    }

    public int getPartition() {
      return partition;
    }

    public String toString() {
      return " PartitionLocations {"
          + "keys: " + keys
          + " , partition: " + partition
          + " , nodes: " + nodes
          + " } ";
    }
  }

  private static class Metadata {

    private final List<KeyQueryMetadata> metadataList;
    private final Optional<Map<Integer, Set<GenericKey>>> keysByPartition;

    Metadata(
        final List<KeyQueryMetadata> metadataList,
        final Optional<Map<Integer, Set<GenericKey>>> keysByPartition
    ) {
      this.metadataList = metadataList;
      this.keysByPartition = keysByPartition;
    }

    public List<KeyQueryMetadata> getMetadataList() {
      return metadataList;
    }

    public Optional<Map<Integer, Set<GenericKey>>> getKeysByPartition() {
      return keysByPartition;
    }
  }
}
