/*
 * Copyright 2026 Confluent Inc.
 */

package io.confluent.ksql.security;

import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;

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
  private final String nodeId;
  private final String clusterId;

  public KsqlResourceExtensionContext(
      final KsqlConfig ksqlConfig,
      final MetricCollectors metricCollectors,
      final String nodeId,
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
  public KsqlConfig ksqlConfig() {
    return ksqlConfig;
  }

  /**
   * @return the server metric collectors; {@code metricCollectors().getMetrics()} yields the Kafka
   *     {@code Metrics} instance an extension can register gauges against
   */
  public MetricCollectors metricCollectors() {
    return metricCollectors;
  }

  /**
   * @return a stable identifier unique to this ksqlDB node
   */
  public String nodeId() {
    return nodeId;
  }

  /**
   * @return the identifier of the ksqlDB cluster this node belongs to
   */
  public String clusterId() {
    return clusterId;
  }
}
