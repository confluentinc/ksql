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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ServiceManager;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.HostStoreLags;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.rest.entity.StateStoreLags;
import io.confluent.ksql.rest.server.HeartbeatAgent.HostStatusListener;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.HostStatus;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.net.URI;
import java.net.URL;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.streams.LagInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent responsible for sending and receiving lag information across the cluster and providing
 * aggregate stats, usable during query time.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class LagReportingAgent implements HostStatusListener {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

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

  private final Map<KsqlHostInfo, HostStoreLags> receivedLagInfo;
  private final AtomicReference<Set<KsqlHostInfo>> aliveHostsRef;

  private URL localUrl;

  /**
   * Builder for creating an instance of LagReportingAgent.
   * @return a builder
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
    this.aliveHostsRef = new AtomicReference<>(Collections.emptySet());
  }

  void setLocalAddress(final String applicationServer) {
    try {
      this.localUrl = new URL(applicationServer);
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
   * @param lagReportingMessage The host lag information sent directly from the other node.
   */
  public void receiveHostLag(final LagReportingMessage lagReportingMessage) {
    final HostStoreLags hostStoreLags = lagReportingMessage.getHostStoreLags();
    final long updateTimeMs = hostStoreLags.getUpdateTimeMs();
    final KsqlHostInfoEntity KsqlHostInfoEntity = lagReportingMessage.getKsqlHost();
    final KsqlHostInfo KsqlHostInfo = KsqlHostInfoEntity.toKsqlHost();

    LOG.debug("Receive lag at: {} from host: {} lag: {} ", updateTimeMs, KsqlHostInfoEntity,
              hostStoreLags.getStateStoreLags());

    receivedLagInfo.compute(KsqlHostInfo, (hi, previousHostLagInfo) ->
        previousHostLagInfo != null && previousHostLagInfo.getUpdateTimeMs() > updateTimeMs
            ? previousHostLagInfo : hostStoreLags);
  }

  /**
   * Returns lag information for all of the "alive" hosts for a given state store and partition.
   * @param queryStateStoreId The lag info key
   * @param partition The partition of that state
   * @return A map which is keyed by host and contains lag information
   */
  public Optional<LagInfoEntity> getLagInfoForHost(
      final KsqlHostInfo host, final QueryStateStoreId queryStateStoreId, final int partition) {
    return getLagPerHost(host)
        .flatMap(hostStoreLags -> hostStoreLags.getStateStoreLags(queryStateStoreId))
        .flatMap(stateStoreLags -> stateStoreLags.getLagByPartition(partition));
  }

  /**
   * Returns a map of host -> store -> partition -> LagInfo.  Meant for being exposed in testing
   * and debug resources.
   */
  public ImmutableMap<KsqlHostInfoEntity, HostStoreLags> getAllLags() {
    return receivedLagInfo.entrySet().stream()
        .collect(ImmutableMap.toImmutableMap(
            e -> new KsqlHostInfoEntity(e.getKey().host(), e.getKey().port()),
            Entry::getValue));
  }

  public Optional<HostStoreLags> getLagPerHost(final KsqlHostInfo host) {
    return Optional.ofNullable(receivedLagInfo.get(host));
  }

  @Override
  public void onHostStatusUpdated(final Map<KsqlHostInfo, HostStatus> hostsStatusMap) {
    aliveHostsRef.set(hostsStatusMap.entrySet().stream()
        .filter(entry -> entry.getValue().isHostAlive())
        .map(Entry::getKey)
        .collect(ImmutableSet.toImmutableSet()));
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
      // Don't let this service fail.  Just keep sending lags
      try {
        sendLagsToAliveHosts();
      } catch (Throwable t) {
        LOG.error("Failed to send lags", t);
      }
    }

    protected void sendLagsToAliveHosts() {
      final List<PersistentQueryMetadata> currentQueries = engine.getPersistentQueries();
      if (currentQueries.isEmpty()) {
        return;
      }

      final Map<QueryStateStoreId, Map<Integer, LagInfo>> localLagMap
          = currentQueries.stream()
          .map(qm -> Pair.of(qm, qm.getAllLocalStorePartitionLags()))
          .map(pair -> pair.getRight().entrySet().stream()
              .collect(Collectors.toMap(
                  e -> QueryStateStoreId.of(pair.getLeft().getQueryId().toString(), e.getKey()),
                  Entry::getValue)))
          .flatMap(map -> map.entrySet().stream())
          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

      final LagReportingMessage message = createLagReportingMessage(localLagMap);

      final Set<KsqlHostInfo> aliveHosts = aliveHostsRef.get();
      for (KsqlHostInfo host: aliveHosts) {
        try {
          final URI remoteUri = ServerUtil.buildRemoteUri(localUrl, host.host(), host.port());
          LOG.debug("Sending lag to host {} at {}", host.host(), clock.millis());
          serviceContext.getKsqlClient().makeAsyncLagReportRequest(remoteUri, message);
        } catch (Throwable t) {
          LOG.error("Request to server: " + host.host() + ":" + host.port()
              + " failed with exception: " + t.getMessage(), t);
        }
      }
    }

    /**
     * Converts between local lag data from Streams and converts it to a HostLagEntity that can be
     * sent to other nodes in the cluster.
     */
    private LagReportingMessage createLagReportingMessage(
        final Map<QueryStateStoreId, Map<Integer, LagInfo>> lagMap) {
      final ImmutableMap.Builder<QueryStateStoreId, StateStoreLags> map
          = ImmutableMap.builder();
      for (Entry<QueryStateStoreId, Map<Integer, LagInfo>> storeEntry : lagMap.entrySet()) {
        final ImmutableMap<Integer, LagInfoEntity> partitionMap
            = storeEntry.getValue().entrySet().stream()
            .map(partitionEntry -> {
              final LagInfo lagInfo = partitionEntry.getValue();
              return Pair.of(partitionEntry.getKey(),
                  new LagInfoEntity(lagInfo.currentOffsetPosition(), lagInfo.endOffsetPosition(),
                  lagInfo.offsetLag()));
            }).collect(ImmutableMap.toImmutableMap(Pair::getLeft, Pair::getRight));
        map.put(storeEntry.getKey(), new StateStoreLags(partitionMap));
      }
      return new LagReportingMessage(new KsqlHostInfoEntity(localUrl.getHost(), localUrl.getPort()),
          new HostStoreLags(map.build(), clock.millis()));
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
}
