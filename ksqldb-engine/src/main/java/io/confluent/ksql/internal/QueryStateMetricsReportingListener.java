/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.internal;

import com.google.common.base.Ticker;
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams.State;

public class QueryStateMetricsReportingListener implements QueryEventListener {

  public static final Ticker CURRENT_TIME_MILLIS_TICKER = new Ticker() {
    @Override
    public long read() {
      return System.currentTimeMillis();
    }
  };

  private final Metrics metrics;
  private final String metricsPrefix;
  private final ConcurrentMap<QueryId, PerQueryListener> perQuery = new ConcurrentHashMap<>();

  QueryStateMetricsReportingListener(final Metrics metrics, final String metricsPrefix) {
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.metricsPrefix
        = Objects.requireNonNull(metricsPrefix, "metricGroupPrefix");
  }

  @Override
  public void onCreate(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final QueryMetadata queryMetadata) {
    if (perQuery.containsKey(queryMetadata.getQueryId())) {
      return;
    }
    perQuery.put(
        queryMetadata.getQueryId(),
        new PerQueryListener(metrics, metricsPrefix, queryMetadata.getQueryId().toString())
    );
  }

  @Override
  public void onStateChange(final QueryMetadata query, final State before, final State after) {
    // this may be called after the query is deregistered, because shutdown is ansynchronous and
    // may time out. when ths happens, the shutdown thread in streams may call this method.
    final PerQueryListener listener = perQuery.get(query.getQueryId());
    if (listener != null) {
      listener.onChange(before, after);
    }
  }

  @Override
  public void onError(final QueryMetadata query, final QueryError error) {
    // this may be called after the query is deregistered, because shutdown is ansynchronous and
    // may time out. when ths happens, the shutdown thread in streams may call this method.
    final PerQueryListener listener = perQuery.get(query.getQueryId());
    if (listener != null) {
      listener.onError(error);
    }
  }

  @Override
  public void onDeregister(final QueryMetadata query) {
    perQuery.get(query.getQueryId()).onDeregister();
    perQuery.remove(query.getQueryId());
  }

  private static final String NO_ERROR = "NO_ERROR";

  private static class PerQueryListener {
    private final Metrics metrics;
    private final MetricName stateMetricName;
    private final MetricName errorMetricName;
    private final Ticker ticker;

    private volatile String state = "-";
    private volatile String error = NO_ERROR;

    PerQueryListener(
        final Metrics metrics,
        final String groupPrefix,
        final String queryId
    ) {
      this(metrics, groupPrefix, queryId, CURRENT_TIME_MILLIS_TICKER);
    }

    PerQueryListener(
        final Metrics metrics,
        final String groupPrefix,
        final String queryId,
        final Ticker ticker
    ) {
      Objects.requireNonNull(groupPrefix, "groupPrefix");
      Objects.requireNonNull(queryId, "queryId");
      this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null.");
      this.ticker = Objects.requireNonNull(ticker, "ticker");

      final String type = queryId.toLowerCase().contains("transient") ? "transient_" : "query_" ;

      final String tag = "_confluent-ksql-" + groupPrefix + type + queryId;

      this.stateMetricName = metrics.metricName(
          "query-status",
          groupPrefix + "ksql-queries",
          "The current status of the given query.",
          Collections.singletonMap("status", tag));

      errorMetricName = metrics.metricName(
          "error-status",
          groupPrefix + "ksql-queries",
          "The current error status of the given query, if the state is in ERROR state",
          Collections.singletonMap("status", tag)
      );
      this.metrics.addMetric(stateMetricName, (Gauge<String>) (config, now) -> state);
      this.metrics.addMetric(errorMetricName, (Gauge<String>) (config, now) -> error);
    }

    public void onChange(final State newState, final State oldState) {
      state = newState.toString();

      if (newState != State.ERROR) {
        error = NO_ERROR;
      }
    }

    public void onError(final QueryError observedError) {
      error = observedError.getType().name();
    }

    public void onDeregister() {
      metrics.removeMetric(stateMetricName);
      metrics.removeMetric(errorMetricName);
    }
  }
}
