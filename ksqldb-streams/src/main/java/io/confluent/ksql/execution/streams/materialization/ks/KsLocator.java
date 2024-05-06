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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.RoutingFilter;
import io.confluent.ksql.execution.streams.RoutingFilter.Host;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.execution.streams.materialization.Locator;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.util.KsqlHostInfo;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Processor;
import org.apache.kafka.streams.TopologyDescription.Source;
import org.apache.kafka.streams.TopologyDescription.Subtopology;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams implementation of {@link Locator}.
 * Uses streams metadata to determine which hosts to contact for a given key/partition.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public final class KsLocator implements Locator {

  private static final Logger LOG = LoggerFactory.getLogger(KsLocator.class);
  private final String storeName;
  private final KafkaStreams kafkaStreams;
  private final Topology topology;
  private final Serializer<GenericKey> keySerializer;
  private final URL localHost;
  private final String applicationId;
  private final boolean sharedRuntimesEnabled;
  private final String queryId;

  KsLocator(
      final String stateStoreName,
      final KafkaStreams kafkaStreams,
      final Topology topology,
      final Serializer<GenericKey> keySerializer,
      final URL localHost,
      final String applicationId,
      final boolean sharedRuntimesEnabled,
      final String queryId
  ) {
    this.kafkaStreams = requireNonNull(kafkaStreams, "kafkaStreams");
    this.topology = requireNonNull(topology, "topology");
    this.keySerializer = requireNonNull(keySerializer, "keySerializer");
    this.storeName = requireNonNull(stateStoreName, "stateStoreName");
    this.localHost = requireNonNull(localHost, "localHost");
    this.applicationId = requireNonNull(applicationId, "applicationId");
    this.sharedRuntimesEnabled = sharedRuntimesEnabled;
    this.queryId = requireNonNull(queryId, "queryId");
  }

  @Override
  public List<KsqlPartitionLocation> locate(
      final List<KsqlKey> keys,
      final RoutingOptions routingOptions,
      final RoutingFilterFactory routingFilterFactory,
      final boolean isRangeScan
  ) {
    if (isRangeScan && keys.isEmpty()) {
      throw new IllegalStateException("Query is range scan but found no range keys.");
    }
    final ImmutableList.Builder<KsqlPartitionLocation> partitionLocations = ImmutableList.builder();
    final Set<Integer> filterPartitions = routingOptions.getPartitions();
    final Optional<Set<KsqlKey>> keySet = keys.isEmpty() ? Optional.empty() :
        Optional.of(Sets.newHashSet(keys));

    // Depending on whether this is a key-based lookup, determine which metadata method to use.
    // If we don't have keys, find the metadata for all partitions since we'll run the query for
    // all partitions of the state store rather than a particular one.
    //For issue #7174. Temporarily turn off metadata finding for a partition with keys
    //if there are more than one key.
    final List<PartitionMetadata> metadata;
    if (keys.size() == 1 && keys.get(0).getKey().size() == 1 && !isRangeScan) {
      metadata = getMetadataForKeys(keys, filterPartitions);
    } else {
      metadata = getMetadataForAllPartitions(filterPartitions, keySet);
    }

    if (metadata.isEmpty()) {
      final MaterializationException materializationException = new MaterializationException(
          "Cannot determine which host contains the required partitions to serve the pull query. \n"
              + "The underlying persistent query may be restarting (e.g. as a result of "
              + "ALTER SYSTEM) view the status of your by issuing <DESCRIBE foo>.");
      LOG.debug(materializationException.getMessage());
      throw materializationException;
    }

    // Go through the metadata and group them by partition.
    for (PartitionMetadata partitionMetadata : metadata) {
      LOG.debug("Handling pull query for partition {} of state store {}.",
                partitionMetadata.getPartition(), storeName);
      final HostInfo activeHost = partitionMetadata.getActiveHost();
      final Set<HostInfo> standByHosts = partitionMetadata.getStandbyHosts();
      final int partition = partitionMetadata.getPartition();
      final Optional<Set<KsqlKey>> partitionKeys = partitionMetadata.getKeys();
      LOG.debug("Active host {}, standby {}, partition {}.",
               activeHost, standByHosts, partition);
      // For a given partition, find the ordered, filtered list of hosts to consider
      final List<KsqlNode> filteredHosts = getFilteredHosts(routingOptions, routingFilterFactory,
          activeHost, standByHosts, partition);

      partitionLocations.add(new PartitionLocation(partitionKeys, partition, filteredHosts));
    }
    return partitionLocations.build();
  }

  /**
   * Gets the Metadata when looking up a list of keys.  This is used when the set of keys are known.
   * @param keys The non-empty set of keys to lookup metadata for
   * @param filterPartitions The partitions to limit lookups to, if non empty. Partitions which
   *                         exist by are not listed here are omitted. If empty, no filtering is
   *                         done.
   * @return The metadata associated with the keys
   */
  private List<PartitionMetadata> getMetadataForKeys(
      final List<KsqlKey> keys,
      final Set<Integer> filterPartitions
  ) {
    // Maintain request order for reproducibility by using a LinkedHashMap, even though it's
    // not a guarantee of the API.
    final Map<Integer, KeyQueryMetadata> metadataByPartition = new LinkedHashMap<>();
    final Map<Integer, Set<KsqlKey>> keysByPartition = new HashMap<>();
    for (KsqlKey key : keys) {
      final KeyQueryMetadata metadata = getKeyQueryMetadata(key);

      // Fail fast if Streams not ready. Let client handle it
      if (metadata.equals(KeyQueryMetadata.NOT_AVAILABLE)) {
        LOG.debug("KeyQueryMetadata not available for state store '{}' and key {}",
                  storeName, key);
        throw new MaterializationException(String.format(
            "Materialized data for key %s is not available yet. "
                + "Please try again later.", key));
      }

      LOG.debug("Handling pull query for key {} in partition {} of state store {}.",
                key, metadata.partition(), storeName);

      if (filterPartitions.size() > 0 && !filterPartitions.contains(metadata.partition())) {
        LOG.debug("Ignoring key {} in partition {} because parition is not included in lookup.",
            key, metadata.partition());
        continue;
      }

      keysByPartition.computeIfAbsent(metadata.partition(), k -> new LinkedHashSet<>());
      keysByPartition.get(metadata.partition()).add(key);
      metadataByPartition.putIfAbsent(metadata.partition(), metadata);
    }

    return metadataByPartition.values().stream()
        .map(metadata -> {
          final HostInfo activeHost = metadata.activeHost();
          final Set<HostInfo> standByHosts = metadata.standbyHosts();
          return new PartitionMetadata(activeHost, standByHosts, metadata.partition(),
              Optional.of(keysByPartition.get(metadata.partition())));
        }).collect(Collectors.toList());
  }

  /**
   * Gets the metadata for all partitions associated with the state store.
   * @param filterPartitions The partitions to limit lookups to, if non empty. Partitions which
   *                         exist by are not listed here are omitted. If empty, no filtering is
   *                         done.
   * @return The metadata associated with all partitions
   */
  private List<PartitionMetadata>  getMetadataForAllPartitions(
      final Set<Integer> filterPartitions,
      final Optional<Set<KsqlKey>> keys
  ) {
    // It's important that we consider only the source topics for the subtopology that contains the
    // state store. Otherwise, we'll be given the wrong partition -> host mappings.
    // The underlying state store has a number of partitions that is the MAX of the number of
    // partitions of all source topics of the subtopology.  Since partition X of all source topics
    // of the particular subtopology will map to the same host, we can collect partition -> host
    // for these topics to find the locations of each partition of the state store.
    final Set<String> sourceTopicSuffixes = findSubtopologySourceTopicSuffixes();
    final Map<Integer, HostInfo> activeHostByPartition = new HashMap<>();
    final Map<Integer, Set<HostInfo>> standbyHostsByPartition = new HashMap<>();
    final Collection<StreamsMetadata> streamsMetadataCollection = getStreamsMetadata();

    for (final StreamsMetadata streamsMetadata : streamsMetadataCollection) {
      streamsMetadata.topicPartitions().forEach(
          tp -> {
            if (sourceTopicSuffixes.stream().anyMatch(suffix -> tp.topic().endsWith(suffix))) {
              activeHostByPartition.compute(tp.partition(), (partition, hostInfo) -> {
                if (hostInfo != null && !streamsMetadata.hostInfo().equals(hostInfo)) {
                  throw new IllegalStateException("Should only be one active host per partition");
                }
                return streamsMetadata.hostInfo();
              });
            }
          });

      streamsMetadata.standbyTopicPartitions().forEach(
          tp -> {
            // Ideally, we'd also throw an exception if we found a mis-mapping for the standbys, but
            // with multiple per partition, we can't easy sanity check.
            if (sourceTopicSuffixes.stream().anyMatch(suffix -> tp.topic().endsWith(suffix))) {
              standbyHostsByPartition.computeIfAbsent(tp.partition(), p -> new HashSet<>());
              standbyHostsByPartition.get(tp.partition()).add(streamsMetadata.hostInfo());
            }
          });
    }

    final Set<Integer> partitions = Streams.concat(
        activeHostByPartition.keySet().stream(),
        standbyHostsByPartition.keySet().stream())
        .collect(Collectors.toSet());

    final List<PartitionMetadata> metadataList = new ArrayList<>();
    for (Integer partition : partitions) {
      if (filterPartitions.size() > 0 && !filterPartitions.contains(partition)) {
        LOG.debug("Ignoring partition {} because partition is not included in lookup.", partition);
        continue;
      }
      final HostInfo activeHost = activeHostByPartition.getOrDefault(partition, UNKNOWN_HOST);
      final Set<HostInfo> standbyHosts = standbyHostsByPartition.getOrDefault(partition,
          Collections.emptySet());
      metadataList.add(
          new PartitionMetadata(activeHost, standbyHosts, partition, keys));
    }
    return metadataList;
  }

  /**
   * Returns KeyQueryMetadata based on whether shared runtimes is enabled
   * @param key KsqlKey
   * @return KeyQueryMetadata
   */
  @VisibleForTesting
  protected KeyQueryMetadata getKeyQueryMetadata(final KsqlKey key) {
    if (sharedRuntimesEnabled && kafkaStreams instanceof KafkaStreamsNamedTopologyWrapper) {
      return ((KafkaStreamsNamedTopologyWrapper) kafkaStreams)
          .queryMetadataForKey(storeName, key.getKey(), keySerializer, queryId);
    }
    return kafkaStreams.queryMetadataForKey(storeName, key.getKey(), keySerializer);
  }

  /**
   * Returns a collection of StreamsMetadata based on whether shared runtimes is enabled.
   * @return Collection of StreamsMetadata
   */
  @VisibleForTesting
  protected Collection<StreamsMetadata> getStreamsMetadata() {
    if (sharedRuntimesEnabled && kafkaStreams instanceof KafkaStreamsNamedTopologyWrapper) {
      return ((KafkaStreamsNamedTopologyWrapper) kafkaStreams)
          .streamsMetadataForStore(storeName, queryId);
    }

    return kafkaStreams.streamsMetadataForStore(storeName);
  }

  /**
   * For the particular state store, this finds the subtopology which contains that store, and
   * then finds the input topics for the subtopology by finding the source nodes. These topics are
   * then used for checking against all the metadata for the state store and used to find the
   * active and standby hosts for the topics. Without doing this check, incorrect assignments could
   * be chosen since different subtopologies can be run by different hosts.
   */
  private Set<String> findSubtopologySourceTopicSuffixes() {
    for (final Subtopology subtopology : topology.describe().subtopologies()) {
      boolean containsStateStore = false;
      for (final TopologyDescription.Node node : subtopology.nodes()) {
        if (node instanceof Processor) {
          final Processor processor = (Processor) node;
          if (processor.stores().contains(storeName)) {
            containsStateStore = true;
          }
        }
      }

      if (!containsStateStore) {
        continue;
      }

      for (final TopologyDescription.Node node : subtopology.nodes()) {
        if (node instanceof Source) {
          final Source source = (Source) node;
          Preconditions.checkNotNull(source.topicSet(), "Expecting topic set, not regex");
          return source.topicSet();
        }
      }
      throw new IllegalStateException("Failed to find source with topics");
    }
    throw new IllegalStateException("Failed to find state store " + storeName);
  }

  /**
   * Returns the filtered, ordered list of nodes which host the given partition. The returned nodes
   * will be contacted to run the query, in order.
   * @param routingOptions The routing options to use when determining the list of nodes
   * @param routingFilterFactory The factory used to create the RoutingFilter used to filter the
   *                             list of nodes
   * @param activeHost Which node is active for the given partition
   * @param standByHosts Which nodes are standbys for the given partition
   * @param partition The partition being located
   * @return The filtered, ordered list of nodes used to run the given query
   */
  private List<KsqlNode> getFilteredHosts(
      final RoutingOptions routingOptions,
      final RoutingFilterFactory routingFilterFactory,
      final HostInfo activeHost,
      final Set<HostInfo> standByHosts,
      final int partition
  ) {
    // If the lookup is for a forwarded request, only filter localhost
    final List<KsqlHostInfo> allHosts;
    if (routingOptions.getIsSkipForwardRequest()) {
      LOG.debug("Before filtering: Local host {} ", localHost);
      allHosts = ImmutableList.of(new KsqlHostInfo(localHost.getHost(), localHost.getPort()));
    } else {
      LOG.debug("Before filtering: Active host {} , standby hosts {}", activeHost, standByHosts);
      allHosts = Stream.concat(Stream.of(activeHost), standByHosts.stream())
          .map(this::asKsqlHost)
          .collect(Collectors.toList());
    }
    final RoutingFilter routingFilter = routingFilterFactory.createRoutingFilter(
        routingOptions,
        allHosts,
        activeHost,
        applicationId,
        storeName,
        partition
    );

    // Filter out hosts based on active, liveness and max lag filters.
    // The list is ordered by routing preference: active node is first, then standby nodes.
    // If heartbeat is not enabled, all hosts are considered alive.
    // If the request is forwarded internally from another ksql server, only the max lag filter
    // is applied.
    final ImmutableList<KsqlNode> filteredHosts = allHosts.stream()
        .map(routingFilter::filter)
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
  KsqlNode asNode(final RoutingFilter.Host host) {
    return new Node(
        isLocalHost(host.getSourceInfo()),
        buildLocation(host.getSourceInfo()),
        host
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
    private final Host host;

    private Node(final boolean local, final URI location, final Host hostInfo) {
      this.local = local;
      this.location = requireNonNull(location, "location");
      this.host = requireNonNull(hostInfo, "hostInfo");
    }

    @Override
    public boolean isLocal() {
      return local;
    }

    @Override
    public URI location() {
      try {
        return new URI(location.toString());
      } catch (final URISyntaxException fatalError) {
        throw new IllegalStateException("Could not deep copy URI: " + location);
      }
    }

    @Override
    public Host getHost() {
      return host;
    }

    @Override
    public String toString() {
      return "Node{"
          + "local = " + local
          + ", location = " + location
          + ", Host = " + host
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
    private final Optional<Set<KsqlKey>> keys;
    private final int partition;
    private final ImmutableList<KsqlNode> nodes;

    public PartitionLocation(final Optional<Set<KsqlKey>> keys, final int partition,
                             final List<KsqlNode> nodes) {
      this.keys = keys;
      this.partition = partition;
      this.nodes = ImmutableList.copyOf(nodes);
    }

    public Optional<Set<KsqlKey>> getKeys() {
      return keys;
    }

    @Override
    public KsqlPartitionLocation removeFilteredHosts() {
      return new PartitionLocation(
          keys,
          partition,
          nodes.stream().filter(node -> node.getHost().isSelected()).collect(Collectors.toList())
      );
    }

    @Override
    public KsqlPartitionLocation removeHeadHost() {
      if (nodes.isEmpty()) {
        return new PartitionLocation(keys, partition, nodes.stream().collect(Collectors.toList()));
      }
      final KsqlNode headNode = nodes.get(0);
      return new PartitionLocation(
        keys,
        partition,
        nodes.stream().filter(node -> !node.equals(headNode)).collect(Collectors.toList())
      );
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "nodes is ImmutableList")
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

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final PartitionLocation that = (PartitionLocation) o;
      return partition == that.partition
          && Objects.equals(keys, that.keys)
          // order does not matter when comparing nodes list, but we don't
          // want to allocate a new set each time `equals` is called, so we
          // just do the O(n^2) containsAll check. n is usually very small
          && nodes.size() == that.nodes.size()
          && nodes.containsAll(that.nodes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keys, partition, nodes);
    }
  }

  /**
   * Metadata kept about a given partition hosting the data we're wanting to fetch
   */
  private static class PartitionMetadata {
    private final HostInfo activeHost;
    private final Set<HostInfo> standbyHosts;
    private final int partition;
    private final Optional<Set<KsqlKey>> keys;

    PartitionMetadata(
        final HostInfo activeHost,
        final Set<HostInfo> standbyHosts,
        final int partition,
        final Optional<Set<KsqlKey>> keys
    ) {
      this.activeHost = activeHost;
      this.standbyHosts = standbyHosts;
      this.partition = partition;
      this.keys = keys;
    }

    /**
     * @return active host for a partition
     */
    public HostInfo getActiveHost() {
      return activeHost;
    }

    /**
     * @return standby hosts for a partition
     */
    public Set<HostInfo> getStandbyHosts() {
      return standbyHosts;
    }

    /**
     * @return the partition
     */
    public int getPartition() {
      return partition;
    }

    /**
     * @return the set of keys associated with the partition, if they exist
     */
    public Optional<Set<KsqlKey>> getKeys() {
      return keys;
    }
  }
}
