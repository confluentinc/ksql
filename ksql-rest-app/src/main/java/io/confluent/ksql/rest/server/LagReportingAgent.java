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

package io.confluent.ksql.rest.server;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingRequest;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.net.URI;
import java.net.URL;
import java.time.Clock;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent responsible for sending and receiving lag information across the cluster and providing
 * aggregate stats, usable during query time.
 */
public final class LagReportingAgent extends AbstractMeshAgent {

  private static final int SEND_HEARTBEAT_DELAY_MS = 100;
  private static final Cache<HostInfoEntity, HostPartitionLagInfo> EMPTY_CACHE = CacheBuilder
      .newBuilder().build();
  private static final Logger LOG = LoggerFactory.getLogger(LagReportingAgent.class);

  private final ServiceContext serviceContext;
  private final LagReportingConfig config;
  private final Clock clock;
  private final Ticker ticker;

  @GuardedBy("receivedLagInfo")
  private final Map<String, Map<Integer, Cache<HostInfoEntity, HostPartitionLagInfo>>>
      receivedLagInfo;
  private final ConcurrentHashMap<String, HostInfoEntity> hosts;

  /**
   * Builder for creating an instance of LagReportingAgent.
   * @return
   */
  public static LagReportingAgent.Builder builder() {
    return new LagReportingAgent.Builder();
  }

  /**
   * Lag related agent for both sending out lag for localhost as well as receiving lag reports for
   * other nodes in the cluster.
   * @param engine The ksql engine to access streams for inferring the cluster and getting local
   *               lags metrics
   * @param serviceContext Service context for issuing ksql requests
   * @param config The LagReportingConfig for configuring this agent
   * @param clock Clock for reporting lag
   * @param ticker Used for keeping track of cache age
   */
  private LagReportingAgent(
      final KsqlEngine engine,
      final ServiceContext serviceContext,
      final LagReportingConfig config,
      final Clock clock,
      final Ticker ticker) {
    super(engine, config);
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.config = requireNonNull(config, "configuration parameters");
    this.clock = clock;
    this.ticker = ticker;
    this.receivedLagInfo = new TreeMap<>();
    this.hosts = new ConcurrentHashMap<>();
  }

  /**
   * Stores the host lag received from a remote Ksql server.
   * @param lagReportingRequest The host lag information sent directly from the other node.
   */
  public void receiveHostLag(final LagReportingRequest lagReportingRequest) {
    synchronized (receivedLagInfo) {
      garbageCollect(lagReportingRequest.getHostInfoEntity());

      for (Map.Entry<String, Map<Integer, LagInfoEntity>> storeEntry
          : lagReportingRequest.getStoreToPartitionToLagMap().entrySet()) {
        String storeName = storeEntry.getKey();
        Map<Integer, LagInfoEntity> partitionMap = storeEntry.getValue();

        receivedLagInfo.computeIfAbsent(storeName, key -> new TreeMap<>());

        // Go through each new partition and add lag info
        for (Map.Entry<Integer, LagInfoEntity> partitionEntry : partitionMap.entrySet()) {
          Integer partition = partitionEntry.getKey();
          LagInfoEntity lagInfo = partitionEntry.getValue();
          receivedLagInfo.get(storeName).computeIfAbsent(partition, key -> createCache());
          receivedLagInfo.get(storeName).get(partition).put(lagReportingRequest.getHostInfoEntity(),
              new HostPartitionLagInfo(lagReportingRequest.getHostInfoEntity(), partition, lagInfo,
                  lagReportingRequest.getLastLagUpdateMs()));
        }
      }
    }
  }

  // Garbage collects all of the partitions and stores that have expired.  Also removed toRemove
  // since it's about to be added.  If this ends up being too costly, it can be done in a background
  // thread.
  //
  // Assumes we're synchronized on receivedLagInfo
  private void garbageCollect(HostInfoEntity toRemove) {
    for (String storeName : receivedLagInfo.keySet()) {
      for (Integer partition : receivedLagInfo.get(storeName).keySet()) {
        receivedLagInfo.get(storeName).get(partition).invalidate(toRemove);
        if (receivedLagInfo.get(storeName).get(partition).size() == 0) {
          receivedLagInfo.get(storeName).remove(partition);
        }
      }
      if (receivedLagInfo.get(storeName).isEmpty()) {
        receivedLagInfo.remove(storeName);
      }
    }
  }

  // Creates a cache to be used for storing lag info by host
  private Cache<HostInfoEntity, HostPartitionLagInfo> createCache() {
    return CacheBuilder.newBuilder()
        .ticker(ticker)
        .expireAfterWrite(config.lagDataExpirationMs, TimeUnit.MILLISECONDS)
        .build();
  }

  /**
   * Returns lag information for all of the hosts for a given state store and partition.
   * @param stateStoreName The state store to consider
   * @param partition The partition of that state
   * @return A map which is keyed by host and contains lag information
   */
  public Map<HostInfoEntity, HostPartitionLagInfo> getHostsPartitionLagInfo(
      String stateStoreName, int partition) {
    synchronized (receivedLagInfo) {
      return ImmutableMap.copyOf(
          receivedLagInfo.getOrDefault(stateStoreName, Collections.emptyMap())
              .getOrDefault(partition, EMPTY_CACHE).asMap());
    }
  }

  /**
   * Returns current positions for
   */
  public Map<String, Map<Integer, Map<String, Long>>> listAllCurrentPositions() {
    synchronized (receivedLagInfo) {
      return receivedLagInfo.entrySet().stream()
          .map(storeNameEntry -> Pair.of(storeNameEntry.getKey(),
              storeNameEntry.getValue().entrySet().stream()
                  .map(partitionEntry -> Pair.<Integer, Map<String, Long>>of(
                      partitionEntry.getKey(),
                      partitionEntry.getValue().asMap().entrySet().stream()
                          .collect(ImmutableSortedMap.toImmutableSortedMap(
                              Comparator.comparing(String::toString),
                              ent -> ent.getKey().toString(),
                              ent -> ent.getValue().getLagInfo().getCurrentOffsetPosition()))))
                  .collect(ImmutableSortedMap.toImmutableSortedMap(
                      Integer::compare, Pair::getLeft, Pair::getRight))))
          .collect(ImmutableSortedMap.toImmutableSortedMap(
              Comparator.comparing(String::toString), Pair::getLeft, Pair::getRight));
    }
  }

  // In addition to the cluster discovery service in the parent class, we also send lag information.
  @Override
  protected List<Service> createServices() {
    return ImmutableList.of(new SendLagService());
  }

  /**
   * Called when an update on cluster hosts comes in
   * @param uniqueHosts The new, updated set of hosts
   */
  @Override
  void onClusterDiscoveryUpdate(Set<HostInfo> uniqueHosts) {
    for (HostInfo hostInfo : uniqueHosts) {
      HostInfoEntity hostInfoEntity = new HostInfoEntity(hostInfo.host(), hostInfo.port());
      final String hostKey = hostInfoEntity.toString();
      hosts.putIfAbsent(hostKey, hostInfoEntity);
    }

    // We could remove old hosts from the receivedLagInfo structure, but for simplicity, we'll
    // assume they'll time out from the cache if the node dies.
  }

  /**
   * Broadcast lags to remote hosts.
   *
   * <p>This is an asynchronous RPC and we do not handle the response returned from the remote
   * server.</p>
   */
  class SendLagService extends AbstractScheduledService {

    @Override
    protected void runOneIteration() {
      final List<PersistentQueryMetadata> currentQueries = engine.getPersistentQueries();
      if (currentQueries.isEmpty()) {
        return;
      }

      final Map<String, Map<Integer, LagInfo>> storeToPartitionToLagMap = currentQueries.stream()
          .map(QueryMetadata::getStoreToPartitionToLagMap)
          .filter(Objects::nonNull)
          .flatMap(map -> map.entrySet().stream())
          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

      LagReportingRequest request = createLagReportingRequest(storeToPartitionToLagMap);

      for (Entry<String, HostInfoEntity> hostInfoEntry: hosts.entrySet()) {
        final String hostString = hostInfoEntry.getKey();
        final HostInfoEntity hostInfo = hostInfoEntry.getValue();
        try {
          if (!hostString.equals(localHostString)) {
            final URI remoteUri = buildRemoteUri(localURL, hostInfo.getHost(), hostInfo.getPort());
            LOG.debug("Sending lag to host {} at {}", hostInfo.getHost(), clock.millis());
            serviceContext.getKsqlClient().makeAsyncLagReportRequest(remoteUri, request);
          }
        } catch (Throwable t) {
          LOG.error("Request to server: " + hostInfo.getHost() + ":" + hostInfo.getPort()
              + " failed with exception: " + t.getMessage(), t);
        }
      }
    }

    /**
     * Converts between local lag data from Streams and converts it to a HostLagEntity that can be
     * sent to other nodes in the cluster.
     */
    private LagReportingRequest createLagReportingRequest(
        final Map<String, Map<Integer, LagInfo>> storeToPartitionToLagMap) {
      Map<String, Map<Integer, LagInfoEntity>> map = new HashMap<>();
      for (Entry<String, Map<Integer, LagInfo>> storeEntry : storeToPartitionToLagMap.entrySet()) {
        Map<Integer, LagInfoEntity> partitionMap = storeEntry.getValue().entrySet().stream()
            .map(partitionEntry -> {
              LagInfo lagInfo = partitionEntry.getValue();
              return Pair.of(partitionEntry.getKey(),
                  new LagInfoEntity(lagInfo.currentOffsetPosition(), lagInfo.endOffsetPosition(),
                  lagInfo.offsetLag()));
            }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
        map.put(storeEntry.getKey(), partitionMap);
      }
      return new LagReportingRequest(new HostInfoEntity(localHostInfo.host(), localHostInfo.port()),
          map, clock.millis());
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(SEND_HEARTBEAT_DELAY_MS,
          config.lagSendIntervalMs,
          TimeUnit.MILLISECONDS);
    }

    @Override
    protected ScheduledExecutorService executor() {
      return scheduledExecutorService;
    }

    /**
     * Constructs a URI for the remote node in the cluster, using the same protocol as localhost.
     * @param localHost Local URL from which to take protocol
     * @param remoteHost The remote host
     * @param remotePort The remote port
     * @return
     */
    private URI buildRemoteUri(final URL localHost, final String remoteHost, final int remotePort) {
      try {
        return new URL(localHost.getProtocol(), remoteHost, remotePort, "/").toURI();
      } catch (final Exception e) {
        throw new IllegalStateException("Failed to convert remote host info to URL."
            + " remoteInfo: " + remoteHost + ":" + remotePort);
      }
    }
  }

  /**
   * Builder for creating a LagReportingAgent.
   */
  public static class Builder {

    // Some defaults set, in case none is given
    private int nestedThreadPoolSize = 2;
    private long nestedLagSendIntervalMs = 500;
    private long nestedDiscoverClusterIntervalMs = 2000;
    private long nestedLagDataExpirationMs = 5000;
    private Clock nestedClock = Clock.systemUTC();
    private Ticker nestedTicker = Ticker.systemTicker();

    LagReportingAgent.Builder threadPoolSize(final int size) {
      nestedThreadPoolSize = size;
      return this;
    }

    LagReportingAgent.Builder lagSendIntervalMs(final long interval) {
      nestedLagSendIntervalMs = interval;
      return this;
    }

    LagReportingAgent.Builder discoverClusterInterval(final long interval) {
      nestedDiscoverClusterIntervalMs = interval;
      return this;
    }

    LagReportingAgent.Builder lagDataExpirationMs(final long lagDataExpirationMs) {
      nestedLagDataExpirationMs = lagDataExpirationMs;
      return this;
    }

    LagReportingAgent.Builder clock(Clock clock) {
      nestedClock = clock;
      return this;
    }

    LagReportingAgent.Builder ticker(Ticker ticker) {
      nestedTicker = ticker;
      return this;
    }

    public LagReportingAgent build(final KsqlEngine engine,
        final ServiceContext serviceContext) {

      return new LagReportingAgent(engine,
          serviceContext,
          new LagReportingConfig(nestedThreadPoolSize,
                                 nestedLagSendIntervalMs,
                                 nestedDiscoverClusterIntervalMs,
                                 nestedLagDataExpirationMs),
          nestedClock,
          nestedTicker);
    }
  }

  static class LagReportingConfig implements AbstractMeshConfig {
    private final int threadPoolSize;
    private final long lagSendIntervalMs;
    private final long lagDataExpirationMs;
    private final long discoverClusterIntervalMs;

    LagReportingConfig(final int threadPoolSize,
                       final long lagSendIntervalMs,
                       final long discoverClusterIntervalMs,
                       final long lagDataExpirationMs) {
      this.threadPoolSize = threadPoolSize;
      this.lagSendIntervalMs = lagSendIntervalMs;
      this.discoverClusterIntervalMs = discoverClusterIntervalMs;
      this.lagDataExpirationMs = lagDataExpirationMs;
    }

    @Override
    public long getDiscoverClusterIntervalMs() {
      return discoverClusterIntervalMs;
    }

    @Override
    public int getThreadPoolSize() {
      return threadPoolSize;
    }
  }

  /**
   * Represents a host, lag information, and when the lag information was collected.
   */
  public static class HostPartitionLagInfo {

    private final HostInfoEntity hostInfo;
    private final int partition;
    private final LagInfoEntity lagInfo;
    private final long updateTimeMs;

    public HostPartitionLagInfo(final HostInfoEntity hostInfo,
                                final int partition,
                                final LagInfoEntity lagInfo,
                                final long updateTimeMs) {
      this.hostInfo = hostInfo;
      this.partition = partition;
      this.lagInfo = lagInfo;
      this.updateTimeMs = updateTimeMs;
    }

    public HostInfoEntity getHostInfo() {
      return hostInfo;
    }

    public LagInfoEntity getLagInfo() {
      return lagInfo;
    }

    public long getCurrentOffsetPosition() {
      return lagInfo.getCurrentOffsetPosition();
    }

    public long getUpdateTimeMs() {
      return updateTimeMs;
    }

    public int getPartition() {
      return partition;
    }
  }
}
