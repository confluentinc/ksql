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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ServiceManager;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingRequest;
import io.confluent.ksql.rest.server.HeartbeatAgent.HeartbeatListener;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.LagInfoKey;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.net.URI;
import java.net.URL;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.LagInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent responsible for sending and receiving lag information across the cluster and providing
 * aggregate stats, usable during query time.
 */
public final class LagReportingAgent implements HeartbeatListener {
  private static final int SERVICE_TIMEOUT_SEC = 2;
  private static final int NUM_THREADS_EXECUTOR = 1;
  private static final int SEND_LAG_DELAY_MS = 100;
  private static final LagCache EMPTY_LAG_CACHE = LagCache.empty();
  private static final Logger LOG = LoggerFactory.getLogger(LagReportingAgent.class);

  private final KsqlEngine engine;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ServiceContext serviceContext;
  private final LagReportingConfig config;
  private final ServiceManager serviceManager;
  private final Clock clock;

  @GuardedBy("receivedLagInfo")
  private final Map<LagInfoKey, Map<Integer, LagCache>>
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
   */
  private LagReportingAgent(
      final KsqlEngine engine,
      final ScheduledExecutorService scheduledExecutorService,
      final ServiceContext serviceContext,
      final LagReportingConfig config,
      final Clock clock) {
    this.engine = requireNonNull(engine, "engine");
    this.scheduledExecutorService = scheduledExecutorService;
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.config = requireNonNull(config, "configuration parameters");
    this.clock = clock;
    this.serviceManager = new ServiceManager(Arrays.asList(new SendLagService()));
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

  void startAgent() {
    try {
      serviceManager.startAsync().awaitHealthy(SERVICE_TIMEOUT_SEC, TimeUnit.SECONDS);
    } catch (TimeoutException | IllegalStateException e) {
      LOG.error("Failed to start heartbeat services with exception " + e.getMessage(), e);
    }
  }

  void stopAgent() {
    try {
      serviceManager.stopAsync().awaitStopped(SERVICE_TIMEOUT_SEC, TimeUnit.SECONDS);
    } catch (TimeoutException | IllegalStateException e) {
      LOG.error("Failed to stop heartbeat services with exception " + e.getMessage(), e);
    } finally {
      scheduledExecutorService.shutdownNow();
    }
  }

  /**
   * Stores the host lag received from a remote Ksql server.
   * @param lagReportingRequest The host lag information sent directly from the other node.
   */
  public void receiveHostLag(final LagReportingRequest lagReportingRequest) {
    final long nowMs = clock.millis();
    synchronized (receivedLagInfo) {
      garbageCollect(lagReportingRequest.getHostInfo());

      for (Map.Entry<String, Map<Integer, LagInfoEntity>> storeEntry
          : lagReportingRequest.getStoreToPartitionToLagMap().entrySet()) {
        final LagInfoKey storeName = LagInfoKey.of(storeEntry.getKey());
        final Map<Integer, LagInfoEntity> partitionMap = storeEntry.getValue();

        receivedLagInfo.computeIfAbsent(storeName, key -> new HashMap<>());

        // Go through each new partition and add lag info
        for (final Map.Entry<Integer, LagInfoEntity> partitionEntry : partitionMap.entrySet()) {
          final Integer partition = partitionEntry.getKey();
          final LagInfoEntity lagInfo = partitionEntry.getValue();
          receivedLagInfo.get(storeName).computeIfAbsent(partition, key -> createCache());
          receivedLagInfo.get(storeName).get(partition).add(
              new HostPartitionLagInfo(lagReportingRequest.getHostInfo(), partition, lagInfo,
                  Math.min(lagReportingRequest.getLastLagUpdateMs(), nowMs)));
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
    final Iterator<Entry<LagInfoKey, Map<Integer, LagCache>>>
        storeIterator = receivedLagInfo.entrySet().iterator();
    while (storeIterator.hasNext()) {
      final Entry<LagInfoKey, Map<Integer, LagCache>> storeEntry =
          storeIterator.next();
      final Iterator<Entry<Integer, LagCache>>
          partitionIterator = storeEntry.getValue().entrySet().iterator();
      while (partitionIterator.hasNext()) {
        final Entry<Integer, LagCache> partitionEntry =
            partitionIterator.next();
        partitionEntry.getValue().remove(toRemove);
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
  private LagCache createCache() {
    return new LagCache(clock, config.lagDataExpirationMs);
  }

  /**
   * Returns lag information for all of the "alive" hosts for a given state store and partition.
   * @param lagInfoKey The lag info key
   * @param partition The partition of that state
   * @return A map which is keyed by host and contains lag information
   */
  public Map<HostInfoEntity, HostPartitionLagInfo> getHostsPartitionLagInfo(
      final LagInfoKey lagInfoKey, final int partition) {
    synchronized (receivedLagInfo) {
      final LagCache partitionLagCache =
          receivedLagInfo.getOrDefault(lagInfoKey, Collections.emptyMap())
              .getOrDefault(partition, EMPTY_LAG_CACHE);
      return partitionLagCache.get().stream()
          .filter(e -> hosts.containsKey(e.getHostInfo().toString()))
          .collect(ImmutableMap.toImmutableMap(
              HostPartitionLagInfo::getHostInfo, Function.identity()));
    }
  }

  /**
   * Returns a map of storeName -> partition -> LagInfoEntity.  Meant for being exposed in testing
   * and debug resources.
   */
  public Map<String, Map<Integer, Map<String, LagInfoEntity>>> listAllLags() {
    synchronized (receivedLagInfo) {
      return receivedLagInfo.entrySet().stream()
          .map(storeNameEntry -> Pair.of(storeNameEntry.getKey().toString(),
              storeNameEntry.getValue().entrySet().stream()
                  .map(partitionEntry -> Pair.<Integer, Map<String, LagInfoEntity>>of(
                        partitionEntry.getKey(), partitionEntry.getValue().get().stream()
                            .collect(ImmutableSortedMap.toImmutableSortedMap(
                                Comparator.comparing(String::toString),
                                ent -> ent.getHostInfo().toString(),
                                ent -> ent.getLagInfo()))))
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
          .filter(Objects::nonNull)
          .flatMap(pmd -> pmd.getStoreToPartitionToLagMap().entrySet().stream()
              .map(e -> Pair.of(e.getKey().toString(), e.getValue())))
          .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

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

    public LagReportingAgent build(final KsqlEngine engine,
        final ServiceContext serviceContext) {
      final ScheduledExecutorService scheduledExecutorService =
          Executors.newScheduledThreadPool(NUM_THREADS_EXECUTOR);
      return new LagReportingAgent(engine,
          scheduledExecutorService,
          serviceContext,
          new LagReportingConfig(nestedLagSendIntervalMs,
                                 nestedLagDataExpirationMs),
          nestedClock);
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
   * Simple "collection" of lags that does expiration and can be easily tested.  This will only
   * hold entries for the number of hosts in a partition, so a simple list is used and complex
   * expiration techniques are avoided.
   */
  public static final class LagCache {
    private List<HostPartitionLagInfo> lagInfos;
    private Clock clock;
    private long lagDataExpirationMs;

    private LagCache(final Clock clock, final long lagDataExpirationMs) {
      this(clock, lagDataExpirationMs, new ArrayList<>(3));
    }

    private LagCache(final Clock clock,
                     final long lagDataExpirationMs,
                     final List<HostPartitionLagInfo> lagInfos) {
      this.clock = clock;
      this.lagDataExpirationMs = lagDataExpirationMs;
      this.lagInfos = lagInfos;
    }

    private void removeExpired() {
      if (lagInfos.size() > 0) {
        final long nowMs = clock.millis();
        lagInfos.removeIf(
            lagInfo -> nowMs - lagInfo.getUpdateTimeMs() >= lagDataExpirationMs);
      }
    }

    public List<HostPartitionLagInfo> get() {
      removeExpired();
      return lagInfos;
    }

    public void remove(final HostInfoEntity hostInfo) {
      lagInfos.removeIf(lagInfo -> lagInfo.getHostInfo().equals(hostInfo));
    }

    public void add(final HostPartitionLagInfo lagInfo) {
      lagInfos.add(lagInfo);
    }

    public int size() {
      removeExpired();
      return lagInfos.size();
    }

    public static LagCache empty() {
      return new LagCache(Clock.systemUTC(), 0, ImmutableList.of());
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

    public long getUpdateTimeMs() {
      return updateTimeMs;
    }

    public int getPartition() {
      return partition;
    }
  }
}
