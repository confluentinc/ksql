/*
 * Copyright 2026 Confluent Inc.
 */

package io.confluent.ksql.security;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable context passed to {@link KsqlResourceExtension#register(KsqlResourceExtensionContext)}.
 *
 * <p>It carries the server {@link MetricCollectors} and this node's identity, which a resource
 * extension needs for cross-cutting concerns such as license node attribution that the plain
 * {@link KsqlResourceExtension#register(KsqlConfig)} overload cannot supply.
 */
public final class KsqlResourceExtensionContext {

  private final KsqlConfig ksqlConfig;
  private final MetricCollectors metricCollectors;
  private final Optional<String> nodeId;
  private final String clusterId;

  public KsqlResourceExtensionContext(
      final KsqlConfig ksqlConfig,
      final MetricCollectors metricCollectors,
      final Optional<String> nodeId,
      final String clusterId
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.metricCollectors = Objects.requireNonNull(metricCollectors, "metricCollectors");
    this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
    this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
  }

  /**
   * @return the ksqlDB configuration containing all server settings
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP",
      justification = "context intentionally shares the live server config")
  public KsqlConfig ksqlConfig() {
    return ksqlConfig;
  }

  /**
   * @return the server metric collectors; {@code metricCollectors().getMetrics()} yields the Kafka
   *     {@code Metrics} instance an extension can register gauges against
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP",
      justification = "context intentionally shares the live server metrics sink")
  public MetricCollectors metricCollectors() {
    return metricCollectors;
  }

  /**
   * @return a stable identifier unique to this ksqlDB node, or empty when the node has no routable
   *     address configured to derive one from. Consumers must treat an empty id as "attribution not
   *     available for this node" rather than substituting a placeholder, since a non-unique id
   *     would collide across nodes.
   */
  public Optional<String> nodeId() {
    return nodeId;
  }

  /**
   * @return the identifier of the ksqlDB cluster this node belongs to
   */
  public String clusterId() {
    return clusterId;
  }
}
