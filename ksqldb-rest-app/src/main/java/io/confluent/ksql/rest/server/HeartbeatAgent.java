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

import com.clearspring.analytics.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ServiceManager;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.util.DiscoverRemoteHostsUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.HostStatus;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.net.URI;
import java.net.URL;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The heartbeat mechanism consists of three periodic tasks running at configurable time intervals:
 * 1. Cluster membership: Discover the Ksql hosts that are part of the cluster.
 * 2. Send heartbeats: Broadcast heartbeats to remote Ksql hosts.
 * 3. Process received heartbeats: Determine which remote host is alive or dead.
 *
 * <p>The services are started in the following order by defining their startup delay:
 * First, the cluster membership service starts, then the sending of the heartbeats and last the
 * processing of the received heartbeats. This provides some buffer for the cluster to be discovered
 * before the processing of heartbeats starts. However, it does not guarantee that a remote
 * server will not be classified as dead immediately after discovered (although we optimistically
 * consider all newly discovered servers as alive) if there is lag in the sending/receiving of
 * heartbeats. That's why the service that sends heartbeats sends to both alive and dead servers:
 * avoid situations where a remote server is classified as dead prematurely.</p>
 *
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class HeartbeatAgent {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final int SERVICE_TIMEOUT_SEC = 2;
  private static final int CHECK_HEARTBEAT_DELAY_MS = 1000;
  private static final int SEND_HEARTBEAT_DELAY_MS = 100;
  private static final int DISCOVER_CLUSTER_DELAY_MS = 50;
  private static final Logger LOG = LogManager.getLogger(HeartbeatAgent.class);

  private final KsqlEngine engine;
  private final ServiceContext serviceContext;
  private final HeartbeatConfig config;
  private final List<HostStatusListener> hostStatusListeners;
  private final ConcurrentHashMap<KsqlHostInfo, TreeMap<Long, HeartbeatInfo>> receivedHeartbeats;
  private final ConcurrentHashMap<KsqlHostInfo, HostStatus> hostsStatus;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ServiceManager serviceManager;
  private final Clock clock;
  private KsqlHostInfo localHost;
  private URL localUrl;

  public static HeartbeatAgent.Builder builder() {
    return new HeartbeatAgent.Builder();
  }

  private HeartbeatAgent(final KsqlEngine engine,
                         final ServiceContext serviceContext,
                         final HeartbeatConfig config,
                         final List<HostStatusListener> hostStatusListeners) {

    this.engine = requireNonNull(engine, "engine");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.config = requireNonNull(config, "configuration parameters");
    this.hostStatusListeners = requireNonNull(hostStatusListeners, "heartbeatListeners");
    this.scheduledExecutorService = Executors.newScheduledThreadPool(config.threadPoolSize);
    this.serviceManager = new ServiceManager(Arrays.asList(
        new DiscoverClusterService(), new SendHeartbeatService(), new CheckHeartbeatService()));
    this.receivedHeartbeats = new ConcurrentHashMap<>();
    this.hostsStatus = new ConcurrentHashMap<>();
    this.clock = Clock.systemUTC();
  }

  /**
   * Stores the heartbeats received from a remote Ksql server.
   * @param hostInfo The host information of the remote Ksql server.
   * @param timestamp The timestamp the heartbeat was sent.
   */
  public void receiveHeartbeat(final KsqlHostInfo hostInfo, final long timestamp) {
    final TreeMap<Long, HeartbeatInfo> heartbeats = receivedHeartbeats.computeIfAbsent(
        hostInfo, key -> new TreeMap<>());
    synchronized (heartbeats) {
      LOG.debug("Receive heartbeat at: {} from host: {} ", timestamp, hostInfo);
      heartbeats.put(timestamp, new HeartbeatInfo(timestamp));
    }
  }

  /**
   * Returns the current view of the cluster containing all hosts discovered (whether alive or dead)
   * @return status of discovered hosts
   */
  public Map<KsqlHostInfo, HostStatus> getHostsStatus() {
    return Collections.unmodifiableMap(hostsStatus);
  }

  @VisibleForTesting
  void setHostsStatus(final Map<KsqlHostInfo, HostStatus> status) {
    hostsStatus.putAll(status);
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

  void setLocalAddress(final String applicationServer) {
    final HostInfo hostInfo = ServerUtil.parseHostInfo(applicationServer);
    this.localHost = new KsqlHostInfo(hostInfo.host(), hostInfo.port());
    try {
      this.localUrl = new URL(applicationServer);
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to convert remote host info to URL."
                                          + " remoteInfo: " + localHost.host() + ":"
                                          + localHost.host());
    }
    //This is called on startup of the heartbeat agent, no other entries should exist in the map
    Preconditions.checkState(hostsStatus.isEmpty(), "expected empty host status map on startup");
    hostsStatus.putIfAbsent(localHost, new HostStatus(true, clock.millis()));
  }

  /**
   * Check the heartbeats received from remote hosts and apply policy to determine whether a host
   * is alive or not.
   */
  class CheckHeartbeatService extends AbstractScheduledService {

    @Override
    protected void runOneIteration() {
      final long now = clock.millis();
      final long windowStart = now - config.heartbeatWindowMs;
      runWithWindow(windowStart, now);
    }

    @VisibleForTesting
    void runWithWindow(final long windowStart, final long windowEnd) {
      try {
        processHeartbeats(windowStart, windowEnd);
      } catch (Throwable t) {
        LOG.error("Failed to process heartbeats for window start = " + windowStart + " end = "
                      + windowEnd + " with exception " + t.getMessage(), t);
      }
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(CHECK_HEARTBEAT_DELAY_MS,
                                            config.heartbeatCheckIntervalMs,
                                            TimeUnit.MILLISECONDS);
    }

    @Override
    protected ScheduledExecutorService executor() {
      return scheduledExecutorService;
    }

    /**
     * If no heartbeats have been received, all previously discovered hosts are marked as dead.
     * If a previously discovered host has not received any heartbeat in this window, it is
     * marked as dead.
     * For all other hosts that received heartbeats, they are processed to determine whether the
     * host is alive or dead based on how many consecutive heartbeats it missed.
     * @param windowStart the start time in ms of the current window
     * @param windowEnd the end time in ms of the current window
     */
    private void processHeartbeats(final long windowStart, final long windowEnd) {

      // No heartbeats received -> mark all hosts as dead
      if (receivedHeartbeats.isEmpty()) {
        hostsStatus.replaceAll((host, status) -> {
          if (!host.equals(localHost)) {
            return status.withHostAlive(false);
          }
          return status;
        });
        notifyListeners();
        return;
      }

      for (Entry<KsqlHostInfo, HostStatus> hostEntry: hostsStatus.entrySet()) {
        final KsqlHostInfo ksqlHostInfo = hostEntry.getKey();
        final HostStatus hostStatus = hostEntry.getValue();
        if (ksqlHostInfo.equals(localHost)) {
          continue;
        }
        final TreeMap<Long, HeartbeatInfo> heartbeats = receivedHeartbeats.get(ksqlHostInfo);
        //For previously discovered hosts, if they have not received any heartbeats, mark them dead
        if (heartbeats == null || heartbeats.isEmpty()) {
          hostsStatus.computeIfPresent(ksqlHostInfo, (host, status) -> status.withHostAlive(false));
        } else {
          final TreeMap<Long, HeartbeatInfo> copy;
          synchronized (heartbeats) {
            LOG.debug("Process heartbeats: {} of host: {}", heartbeats, ksqlHostInfo);
            // 1. remove heartbeats older than window
            heartbeats.headMap(windowStart).clear();
            copy = new TreeMap<>(heartbeats.subMap(windowStart, true, windowEnd, true));
            LOG.debug("Process heartbeats: {} of host: {}, window start: {}, window end: {}",
                      copy, ksqlHostInfo, windowStart, windowEnd);
          }
          // 2. count consecutive missed heartbeats and mark as alive or dead
          final  boolean isAlive = decideStatus(ksqlHostInfo, windowStart, windowEnd, copy);
          if (!isAlive) {
            LOG.info("Host: {} marked as dead.", ksqlHostInfo);
          }
          hostsStatus.computeIfPresent(ksqlHostInfo, (host, status) -> status
              .withHostAlive(isAlive).withLastStatusUpdateMs(windowEnd));
        }
      }
      notifyListeners();
    }

    private void notifyListeners() {
      for (HostStatusListener listener : hostStatusListeners) {
        try {
          listener.onHostStatusUpdated(getHostsStatus());
        } catch (Throwable t) {
          LOG.error("Error while notifying listener", t);
        }
      }
    }

    private boolean decideStatus(
        final KsqlHostInfo ksqlHostInfo, final long windowStart, final long windowEnd,
        final TreeMap<Long, HeartbeatInfo> heartbeats
    ) {
      long missedCount = 0;
      long prev = windowStart;
      // No heartbeat received in this window
      if (heartbeats.isEmpty()) {
        return false;
      }
      // We want to count consecutive missed heartbeats and reset the count when we have received
      // heartbeats. It's not enough to just count how many heartbeats we missed in the window as a
      // host may have missed > THRESHOLD but not consecutive ones which doesn't constitute it
      // as dead.
      for (long ts : heartbeats.keySet()) {
        //Don't count heartbeats after window end
        if (ts >= windowEnd) {
          break;
        }
        if (ts - config.heartbeatSendIntervalMs > prev) {
          missedCount = (ts - prev - 1) / config.heartbeatSendIntervalMs;
          LOG.debug("Host: {} missed: {} heartbeats, current heartbeat: {}, previous heartbeat: {},"
                        + " send interval: {}.",
                    ksqlHostInfo, missedCount, ts, prev, config.heartbeatSendIntervalMs);
        } else {
          //Reset missed count when we receive heartbeat
          missedCount = 0;
        }
        prev = ts;
      }
      // Check frame from last received heartbeat to window end
      if (windowEnd - prev - 1 > 0) {
        missedCount = (windowEnd - prev - 1) / config.heartbeatSendIntervalMs;
        LOG.debug("Host: {} missed: {} heartbeats, window end: {}, previous heartbeat: {},"
                      + " send interval: {}.",
                  ksqlHostInfo, missedCount, windowEnd, prev, config.heartbeatSendIntervalMs);
      }
      LOG.debug("Host: {} has {} missing heartbeats", ksqlHostInfo, missedCount);
      return (missedCount < config.heartbeatMissedThreshold);
    }
  }

  /**
   * Broadcast heartbeats to remote hosts whether they are alive or not.
   * We are sending to hosts that might be dead because at startup, a host maybe marked as dead
   * only because the sending of heartbeats has not preceded.
   *
   * <p>This is an asynchronous RPC and we do not handle the response returned from the remote
   * server.</p>
   */
  class SendHeartbeatService extends AbstractScheduledService {

    @Override
    protected void runOneIteration() {
      for (Entry<KsqlHostInfo, HostStatus> hostStatusEntry: hostsStatus.entrySet()) {
        final KsqlHostInfo remoteHost = hostStatusEntry.getKey();
        try {
          if (!remoteHost.equals(localHost)) {
            final URI remoteUri = ServerUtil.buildRemoteUri(
                localUrl, remoteHost.host(), remoteHost.port());
            LOG.debug("Send heartbeat to host {} at {}", remoteHost, clock.millis());
            serviceContext.getKsqlClient().makeAsyncHeartbeatRequest(
                remoteUri, localHost, clock.millis());
          }
        } catch (Throwable t) {
          LOG.error("Request to server: " + remoteHost + " failed with exception: "
                        + t.getMessage(), t);
        }
      }
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(SEND_HEARTBEAT_DELAY_MS,
                                            config.heartbeatSendIntervalMs,
                                            TimeUnit.MILLISECONDS);
    }

    @Override
    protected ScheduledExecutorService executor() {
      return scheduledExecutorService;
    }
  }

  /**
   * Discovers remote hosts in the cluster through the metadata of currently running
   * persistent queries.
   */
  class DiscoverClusterService extends AbstractScheduledService {

    @Override
    protected void runOneIteration() {
      try {
        final List<PersistentQueryMetadata> currentQueries = engine.getPersistentQueries();
        if (currentQueries.isEmpty()) {
          return;
        }
        final Set<HostInfo> uniqueHosts =
            DiscoverRemoteHostsUtil.getRemoteHosts(currentQueries, localHost);

        for (HostInfo hostInfo : uniqueHosts) {
          // Only add to map if it is the first time it is discovered. Design decision to
          // optimistically consider every newly discovered server as alive to avoid situations of
          // unavailability until the heartbeating kicks in.
          final KsqlHostInfo host = new KsqlHostInfo(hostInfo.host(), hostInfo.port());
          hostsStatus.computeIfAbsent(host, key -> new HostStatus(true, clock.millis()));
        }
      } catch (Throwable t) {
        LOG.error("Failed to discover cluster with exception " + t.getMessage(), t);
      }
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(
          DISCOVER_CLUSTER_DELAY_MS,
          config.discoverClusterIntervalMs,
          TimeUnit.MILLISECONDS);
    }

    @Override
    protected ScheduledExecutorService executor() {
      return scheduledExecutorService;
    }
  }

  public static class Builder {

    private int nestedThreadPoolSize;
    private long nestedHeartbeatSendIntervalMs;
    private long nestedHeartbeatCheckIntervalMs;
    private long nestedDiscoverClusterIntervalMs;
    private long nestedHeartbeatWindowMs;
    private long nestedHeartbeatMissedThreshold;
    private List<HostStatusListener> nestedHostStatusListeners = Lists.newArrayList();

    HeartbeatAgent.Builder threadPoolSize(final int size) {
      nestedThreadPoolSize = size;
      return this;
    }

    HeartbeatAgent.Builder heartbeatSendInterval(final long interval) {
      nestedHeartbeatSendIntervalMs = interval;
      return this;
    }

    HeartbeatAgent.Builder heartbeatCheckInterval(final long interval) {
      nestedHeartbeatCheckIntervalMs = interval;
      return this;
    }

    HeartbeatAgent.Builder heartbeatWindow(final long window) {
      nestedHeartbeatWindowMs = window;
      return this;
    }

    HeartbeatAgent.Builder heartbeatMissedThreshold(final long missed) {
      nestedHeartbeatMissedThreshold = missed;
      return this;
    }

    HeartbeatAgent.Builder discoverClusterInterval(final long interval) {
      nestedDiscoverClusterIntervalMs = interval;
      return this;
    }

    HeartbeatAgent.Builder addHostStatusListener(final HostStatusListener listener) {
      nestedHostStatusListeners.add(listener);
      return this;
    }

    public HeartbeatAgent build(final KsqlEngine engine,
                                final ServiceContext serviceContext) {

      return new HeartbeatAgent(engine,
                                serviceContext,
                                new HeartbeatConfig(nestedThreadPoolSize,
                                                      nestedHeartbeatSendIntervalMs,
                                                      nestedHeartbeatCheckIntervalMs,
                                                      nestedHeartbeatWindowMs,
                                                      nestedHeartbeatMissedThreshold,
                                                      nestedDiscoverClusterIntervalMs),
          nestedHostStatusListeners);
    }
  }

  static class HeartbeatConfig {
    private final int threadPoolSize;
    private final long heartbeatSendIntervalMs;
    private final long heartbeatCheckIntervalMs;
    private final long heartbeatWindowMs;
    private final long heartbeatMissedThreshold;
    private final long discoverClusterIntervalMs;

    HeartbeatConfig(final int threadPoolSize, final long heartbeatSendIntervalMs,
                    final long heartbeatCheckIntervalMs, final long heartbeatWindowMs,
                    final long heartbeatMissedThreshold, final long discoverClusterIntervalMs) {
      this.threadPoolSize = threadPoolSize;
      this.heartbeatSendIntervalMs = heartbeatSendIntervalMs;
      this.heartbeatCheckIntervalMs = heartbeatCheckIntervalMs;
      this.heartbeatWindowMs = heartbeatWindowMs;
      this.heartbeatMissedThreshold = heartbeatMissedThreshold;
      this.discoverClusterIntervalMs = discoverClusterIntervalMs;
    }
  }

  public static class HeartbeatInfo {
    private final long timestamp;

    public HeartbeatInfo(final long timestamp) {
      this.timestamp = timestamp;
    }

    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public String toString() {
      return String.valueOf(timestamp);
    }
  }

  /**
   * A listener for heartbeat related events.
   */
  public interface HostStatusListener {

    /**
     * Call when the map of host statuses are updated
     * @param hostsStatusMap The new host status map
     */
    void onHostStatusUpdated(Map<KsqlHostInfo, HostStatus> hostsStatusMap);
  }
}
