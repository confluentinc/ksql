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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.state.HostInfo;
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
  private static final Logger LOG = LoggerFactory.getLogger(LagReportingAgent.class);

  private final KsqlEngine engine;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ServiceContext serviceContext;
  private final LagReportingConfig config;
  private final ServiceManager serviceManager;
  private final Clock clock;

  @GuardedBy("receivedLagInfo")
  private final Map<HostInfo, Map<LagInfoKey, Map<Integer, HostPartitionLagInfo>>>
      receivedLagInfo;
  private final Map<HostInfo, Boolean> aliveHosts;

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
    this.receivedLagInfo = new ConcurrentHashMap<>();
    this.aliveHosts = new ConcurrentHashMap<>();
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
    final HostInfoEntity hostInfoEntity = lagReportingRequest.getHostInfo();
    final HostInfo hostInfo = new HostInfo(hostInfoEntity.getHost(), hostInfoEntity.getPort());
    receivedLagInfo.remove(hostInfo);
    Map<LagInfoKey, Map<Integer, HostPartitionLagInfo>> hostMap =
        receivedLagInfo.computeIfAbsent(hostInfo, key -> new ConcurrentHashMap<>());

    for (Map.Entry<String, Map<Integer, LagInfoEntity>> storeEntry
        : lagReportingRequest.getStoreToPartitionToLagMap().entrySet()) {
      final LagInfoKey lagInfoKey = LagInfoKey.of(storeEntry.getKey());
      final Map<Integer, LagInfoEntity> partitionMap = storeEntry.getValue();

      hostMap.computeIfAbsent(lagInfoKey, key -> new ConcurrentHashMap<>());

      // Go through each new partition and add lag info
      for (final Map.Entry<Integer, LagInfoEntity> partitionEntry : partitionMap.entrySet()) {
        final Integer partition = partitionEntry.getKey();
        final LagInfoEntity lagInfo = partitionEntry.getValue();
        hostMap.get(lagInfoKey).put(partition,
            new HostPartitionLagInfo(hostInfo, partition, lagInfo,
                Math.min(lagReportingRequest.getLastLagUpdateMs(), nowMs)));
      }
    }
  }

  /**
   * Returns lag information for all of the "alive" hosts for a given state store and partition.
   * @param lagInfoKey The lag info key
   * @param partition The partition of that state
   * @return A map which is keyed by host and contains lag information
   */
  public Map<HostInfo, HostPartitionLagInfo> getHostsPartitionLagInfo(
      final Set<HostInfo> hosts, final LagInfoKey lagInfoKey, final int partition) {
    ImmutableMap.Builder<HostInfo, HostPartitionLagInfo> builder = ImmutableMap.builder();
    for (HostInfo host : hosts) {
      HostPartitionLagInfo lagInfo = receivedLagInfo.getOrDefault(host, Collections.emptyMap())
          .getOrDefault(lagInfoKey, Collections.emptyMap())
          .getOrDefault(partition, null);
      if (aliveHosts.containsKey(host) && lagInfo != null) {
        builder.put(host, lagInfo);
      }
    }
    return builder.build();
  }

  /**
   * Returns a map of storeName -> partition -> LagInfoEntity.  Meant for being exposed in testing
   * and debug resources.
   */
  public Map<String, Map<String, Map<Integer, LagInfoEntity>>> listAllLags() {
      return receivedLagInfo.entrySet().stream()
          .map(hostEntry -> Pair.of(
              hostEntry.getKey().host() + ":" + hostEntry.getKey().port(),
              hostEntry.getValue().entrySet().stream()
                  .map(storeNameEntry -> Pair.<String, Map<Integer, LagInfoEntity>>of(
                      storeNameEntry.getKey().toString(),
                      storeNameEntry.getValue().entrySet().stream()
                          .map(partitionEntry -> Pair.of(
                                partitionEntry.getKey(), partitionEntry.getValue().getLagInfo()))
                          .collect(ImmutableSortedMap.toImmutableSortedMap(
                              Integer::compare, Pair::getLeft, Pair::getRight))))
                  .collect(ImmutableSortedMap.toImmutableSortedMap(
                      Comparator.comparing(String::toString), Pair::getLeft, Pair::getRight))))
          .collect(ImmutableSortedMap.toImmutableSortedMap(
              Comparator.comparing(String::toString), Pair::getLeft, Pair::getRight));
  }

  @Override
  public void onHostStatusUpdated(final Map<String, HostStatusEntity> hostsStatusMap) {
    aliveHosts.clear();
    aliveHosts.putAll(hostsStatusMap.values().stream()
        .filter(HostStatusEntity::getHostAlive)
        .map(HostStatusEntity::getHostInfoEntity)
        .map(hostInfoEntity -> new HostInfo(hostInfoEntity.getHost(), hostInfoEntity.getPort()))
        .collect(Collectors.toMap(Function.identity(), hi -> true)));
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

      for (Entry<HostInfo, Boolean> hostInfoEntry: aliveHosts.entrySet()) {
        final HostInfo hostInfo = hostInfoEntry.getKey();
        try {
          final URI remoteUri = buildRemoteUri(localURL, hostInfo.host(), hostInfo.port());
          LOG.debug("Sending lag to host {} at {}", hostInfo.host(), clock.millis());
          serviceContext.getKsqlClient().makeAsyncLagReportRequest(remoteUri, request);
        } catch (Throwable t) {
          LOG.error("Request to server: " + hostInfo.host() + ":" + hostInfo.port()
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
    private Clock nestedClock = Clock.systemUTC();

    LagReportingAgent.Builder lagSendIntervalMs(final long interval) {
      nestedLagSendIntervalMs = interval;
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
          new LagReportingConfig(nestedLagSendIntervalMs),
          nestedClock);
    }
  }

  static class LagReportingConfig {
    private final long lagSendIntervalMs;

    LagReportingConfig(final long lagSendIntervalMs) {
      this.lagSendIntervalMs = lagSendIntervalMs;
    }
  }

  /**
   * Represents a host, lag information, and when the lag information was collected.
   */
  public static class HostPartitionLagInfo {

    private final HostInfo hostInfo;
    private final int partition;
    private final LagInfoEntity lagInfo;
    private final long updateTimeMs;

    public HostPartitionLagInfo(final HostInfo hostInfo,
                                final int partition,
                                final LagInfoEntity lagInfo,
                                final long updateTimeMs) {
      this.hostInfo = hostInfo;
      this.partition = partition;
      this.lagInfo = lagInfo;
      this.updateTimeMs = updateTimeMs;
    }

    public HostInfo getHostInfo() {
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

    @Override
    public String toString() {
      return lagInfo.toString();
    }
  }
}
