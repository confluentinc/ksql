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

import static java.util.Objects.requireNonNull;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingRequest;
import io.confluent.ksql.rest.server.HeartbeatAgent.HeartbeatListener;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.streams.LagInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent responsible for sending and receiving lag information across the cluster and providing
 * aggregate stats, usable during query time.
 */
public final class LagReportingAgent implements HeartbeatListener {
  private static final int NUM_THREADS_EXECUTOR = 1;
  private static final int SEND_LAG_DELAY_MS = 100;
  private static final Cache<HostInfoEntity, HostPartitionLagInfo> EMPTY_CACHE = CacheBuilder
      .newBuilder().build();
  private static final Logger LOG = LoggerFactory.getLogger(LagReportingAgent.class);

  private final KsqlEngine engine;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ServiceContext serviceContext;
  private final LagReportingConfig config;
  private final Clock clock;
  private final Ticker ticker;

  @GuardedBy("receivedLagInfo")
  private final Map<String, Map<Integer, Cache<HostInfoEntity, HostPartitionLagInfo>>>
      receivedLagInfo;
  private final ConcurrentHashMap<String, HostInfoEntity> hosts;

  private URL localURL;

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
      final ScheduledExecutorService scheduledExecutorService,
      final ServiceContext serviceContext,
      final LagReportingConfig config,
      final Clock clock,
      final Ticker ticker) {
    this.engine = requireNonNull(engine, "engine");
    this.scheduledExecutorService = scheduledExecutorService;
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.config = requireNonNull(config, "configuration parameters");
    this.clock = clock;
    this.ticker = ticker;
    this.receivedLagInfo = new HashMap<>();
    this.hosts = new ConcurrentHashMap<>();
  }

  void setLocalAddress(final String applicationServer) {
    try {
      this.localURL = new URL(applicationServer);
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to convert remote host info to URL."
          + " remoteInfo: " + applicationServer);
    }
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
        final String storeName = storeEntry.getKey();
        final Map<Integer, LagInfoEntity> partitionMap = storeEntry.getValue();

        receivedLagInfo.computeIfAbsent(storeName, key -> new HashMap<>());

        // Go through each new partition and add lag info
        for (final Map.Entry<Integer, LagInfoEntity> partitionEntry : partitionMap.entrySet()) {
          final Integer partition = partitionEntry.getKey();
          final LagInfoEntity lagInfo = partitionEntry.getValue();
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
  private void garbageCollect(final HostInfoEntity toRemove) {
    final Iterator<Entry<String, Map<Integer, Cache<HostInfoEntity, HostPartitionLagInfo>>>>
        storeIterator = receivedLagInfo.entrySet().iterator();
    while (storeIterator.hasNext()) {
      final Entry<String, Map<Integer, Cache<HostInfoEntity, HostPartitionLagInfo>>> storeEntry =
          storeIterator.next();
      final Iterator<Entry<Integer, Cache<HostInfoEntity, HostPartitionLagInfo>>>
          partitionIterator = storeEntry.getValue().entrySet().iterator();
      while (partitionIterator.hasNext()) {
        final Entry<Integer, Cache<HostInfoEntity, HostPartitionLagInfo>> partitionEntry =
            partitionIterator.next();
        partitionEntry.getValue().invalidate(toRemove);
        if (partitionEntry.getValue().size() == 0) {
          partitionIterator.remove();
        }
      }
      if (storeEntry.getValue().isEmpty()) {
        storeIterator.remove();
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
   * Returns lag information for all of the "alive" hosts for a given state store and partition.
   * @param stateStoreName The state store to consider
   * @param partition The partition of that state
   * @return A map which is keyed by host and contains lag information
   */
  public Map<HostInfoEntity, HostPartitionLagInfo> getHostsPartitionLagInfo(
      final String stateStoreName, final int partition) {
    synchronized (receivedLagInfo) {
      final Map<HostInfoEntity, HostPartitionLagInfo> partitionLagInfoMap =
          receivedLagInfo.getOrDefault(stateStoreName, Collections.emptyMap())
              .getOrDefault(partition, EMPTY_CACHE).asMap();
      return partitionLagInfoMap.entrySet().stream()
          .filter(e -> hosts.containsKey(e.getKey().toString()))
          .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));
    }
  }

  /**
   * Returns current positions for
   */
  public Map<String, Map<Integer, Map<String, LagInfoEntity>>> listAllLags() {
    synchronized (receivedLagInfo) {
      return receivedLagInfo.entrySet().stream()
          .map(storeNameEntry -> Pair.of(storeNameEntry.getKey(),
              storeNameEntry.getValue().entrySet().stream()
                  .map(partitionEntry -> Pair.<Integer, Map<String, LagInfoEntity>>of(
                      partitionEntry.getKey(),
                      partitionEntry.getValue().asMap().entrySet().stream()
                          .collect(ImmutableSortedMap.toImmutableSortedMap(
                              Comparator.comparing(String::toString),
                              ent -> ent.getKey().toString(),
                              ent -> ent.getValue().getLagInfo()))))
                  .collect(ImmutableSortedMap.toImmutableSortedMap(
                      Integer::compare, Pair::getLeft, Pair::getRight))))
          .collect(ImmutableSortedMap.toImmutableSortedMap(
              Comparator.comparing(String::toString), Pair::getLeft, Pair::getRight));
    }
  }

  @Override
  public void onHostStatusUpdated(final Map<String, HostStatusEntity> hostsStatusMap) {
    hosts.clear();
    hostsStatusMap.entrySet().stream()
        .filter(e -> e.getValue().getHostAlive())
        .map(e -> e.getValue().getHostInfoEntity())
        .forEach(hostInfo -> hosts.put(hostInfo.toString(), hostInfo));
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

      final LagReportingRequest request = createLagReportingRequest(storeToPartitionToLagMap);

      for (Entry<String, HostInfoEntity> hostInfoEntry: hosts.entrySet()) {
        final HostInfoEntity hostInfo = hostInfoEntry.getValue();
        try {
          final URI remoteUri = buildRemoteUri(localURL, hostInfo.getHost(), hostInfo.getPort());
          LOG.debug("Sending lag to host {} at {}", hostInfo.getHost(), clock.millis());
          serviceContext.getKsqlClient().makeAsyncLagReportRequest(remoteUri, request);
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
      final Map<String, Map<Integer, LagInfoEntity>> map = new HashMap<>();
      for (Entry<String, Map<Integer, LagInfo>> storeEntry : storeToPartitionToLagMap.entrySet()) {
        final Map<Integer, LagInfoEntity> partitionMap = storeEntry.getValue().entrySet().stream()
            .map(partitionEntry -> {
              final LagInfo lagInfo = partitionEntry.getValue();
              return Pair.of(partitionEntry.getKey(),
                  new LagInfoEntity(lagInfo.currentOffsetPosition(), lagInfo.endOffsetPosition(),
                  lagInfo.offsetLag()));
            }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
        map.put(storeEntry.getKey(), partitionMap);
      }
      return new LagReportingRequest(new HostInfoEntity(localURL.getHost(), localURL.getPort()),
          map, clock.millis());
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(SEND_LAG_DELAY_MS,
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
    private long nestedLagSendIntervalMs = 500;
    private long nestedLagDataExpirationMs = 5000;
    private Clock nestedClock = Clock.systemUTC();
    private Ticker nestedTicker = Ticker.systemTicker();

    LagReportingAgent.Builder lagSendIntervalMs(final long interval) {
      nestedLagSendIntervalMs = interval;
      return this;
    }

    LagReportingAgent.Builder lagDataExpirationMs(final long lagDataExpirationMs) {
      nestedLagDataExpirationMs = lagDataExpirationMs;
      return this;
    }

    LagReportingAgent.Builder clock(final Clock clock) {
      nestedClock = clock;
      return this;
    }

    LagReportingAgent.Builder ticker(final Ticker ticker) {
      nestedTicker = ticker;
      return this;
    }

    public LagReportingAgent build(final KsqlEngine engine,
        final ServiceContext serviceContext) {
      final ScheduledExecutorService scheduledExecutorService =
          Executors.newScheduledThreadPool(NUM_THREADS_EXECUTOR);
      return new LagReportingAgent(engine,
          scheduledExecutorService,
          serviceContext,
          new LagReportingConfig(nestedLagSendIntervalMs,
                                 nestedLagDataExpirationMs),
          nestedClock,
          nestedTicker);
    }
  }

  static class LagReportingConfig {
    private final long lagSendIntervalMs;
    private final long lagDataExpirationMs;

    LagReportingConfig(final long lagSendIntervalMs,
                       final long lagDataExpirationMs) {
      this.lagSendIntervalMs = lagSendIntervalMs;
      this.lagDataExpirationMs = lagDataExpirationMs;
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
