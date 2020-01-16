package io.confluent.ksql.rest.server;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMeshAgent {

  private static final int SERVICE_TIMEOUT_SEC = 2;
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMeshAgent.class);

  protected final KsqlEngine engine;
  protected final ScheduledExecutorService scheduledExecutorService;
  private final AbstractMeshConfig config;

  protected HostInfo localHostInfo;
  protected String localHostString;
  protected URL localURL;
  private ServiceManager serviceManager;

  public AbstractMeshAgent(final KsqlEngine engine,
      final AbstractMeshConfig config) {
    this.engine = requireNonNull(engine, "engine");
    this.config = requireNonNull(config, "config");
    this.scheduledExecutorService = Executors.newScheduledThreadPool(config.getThreadPoolSize());
  }

  /**
   * Starts the services which are part of this agent.
   */
  void startAgent() {
    try {
      this.serviceManager = new ServiceManager(
          ImmutableList.<Service>builder().addAll(createServices())
              .add(new DiscoverClusterService()).build());
      serviceManager.startAsync().awaitHealthy(SERVICE_TIMEOUT_SEC, TimeUnit.SECONDS);
    } catch (TimeoutException | IllegalStateException e) {
      LOG.error("Failed to start heartbeat services with exception " + e.getMessage(), e);
    }
  }

  /**
   * Stops the services which are part of this agent.
   */
  void stopAgent() {
    try {
      if (serviceManager != null) {
        serviceManager.stopAsync().awaitStopped(SERVICE_TIMEOUT_SEC, TimeUnit.SECONDS);
      }
      scheduledExecutorService.shutdown();
    } catch (TimeoutException | IllegalStateException e) {
      LOG.error("Failed to stop heartbeat services with exception " + e.getMessage(), e);
    }
  }

  /**
   * Sets the local address to be used during cluster discovery, so we know which node is this one.
   * @param applicationServer The endpoint for the application server
   */
  public void setLocalAddress(final String applicationServer) {
    try {
      this.localURL = new URL(applicationServer);
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to convert remote host info to URL."
          + " remoteInfo: " + applicationServer);
    }
    this.localHostInfo = parseHostInfo(localURL);
    this.localHostString = localHostInfo.toString();
  }

  private static HostInfo parseHostInfo(final URL url) {
    final String host = url.getHost();
    final int port = url.getPort();

    return new HostInfo(host, port);
  }

  /**
   * Should be implemented by subclasses.  The subclass should create any services it wants to be
   * run alongside the cluster discovery
   * @return
   */
  abstract List<Service> createServices();

  abstract void onClusterDiscoveryUpdate(final Set<HostInfo> uniqueHosts);

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

        final Set<HostInfo> uniqueHosts = currentQueries.stream()
            .map(queryMetadata -> ((QueryMetadata) queryMetadata).getAllMetadata())
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .map(StreamsMetadata::hostInfo)
            .filter(hostInfo -> !(hostInfo.host().equals(localHostInfo.host())
                && hostInfo.port() == (localHostInfo.port())))
            .collect(Collectors.toSet());

        onClusterDiscoveryUpdate(uniqueHosts);
      } catch (Throwable t) {
        LOG.error("Failed to discover cluster with exception " + t.getMessage(), t);
      }
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(0, config.getDiscoverClusterIntervalMs(),
          TimeUnit.MILLISECONDS);
    }

    @Override
    protected ScheduledExecutorService executor() {
      return scheduledExecutorService;
    }
  }

  public interface AbstractMeshConfig {
    long getDiscoverClusterIntervalMs();
    int getThreadPoolSize();
  }

}
