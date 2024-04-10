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
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.QueryMetadata;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.streams.KafkaStreams.State;

public class QueryStateMetricsReportingListener implements QueryEventListener {
  public static final String QUERY_RESTART_METRIC_NAME = "query-restart-total";
  public static final String QUERY_RESTART_METRIC_DESCRIPTION =
      "The total number of times that a query thread has failed and then been restarted.";

  public static final Ticker CURRENT_TIME_MILLIS_TICKER = new Ticker() {
    @Override
    public long read() {
      return System.currentTimeMillis();
    }
  };

  private final Metrics metrics;
  private final String metricsPrefix;
  private final Map<String, String> metricsTags;
  private final ConcurrentMap<QueryId, PerQueryListener> perQuery = new ConcurrentHashMap<>();

  QueryStateMetricsReportingListener(
      final Metrics metrics,
      final String metricsPrefix,
      final Map<String, String> metricsTags
  ) {
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.metricsPrefix
        = Objects.requireNonNull(metricsPrefix, "metricGroupPrefix");
    this.metricsTags = Objects.requireNonNull(metricsTags);
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
        new PerQueryListener(
            metrics,
            metricsPrefix,
            queryMetadata.getQueryId().toString(),
            metricsTags
        )
    );
  }

  @Override
  public void onStateChange(final QueryMetadata query, final State before, final State after) {
    // this may be called after the query is deregistered, because shutdown is ansynchronous and
    // may time out. when ths happens, the shutdown thread in streams may call this method.
    final PerQueryListener listener = perQuery.get(query.getQueryId());
    if (listener != null) {
      listener.onChange(before, after);
      listener.setKsqlQueryState(query.getQueryStatus().toString());
    }
  }

  @Override
  public void onKsqlStateChange(final QueryMetadata query) {
    final PerQueryListener listener = perQuery.get(query.getQueryId());
    if (listener != null) {
      listener.setKsqlQueryState(query.getQueryStatus().toString());
    }
  }

  @Override
  public void onError(final QueryMetadata query, final QueryError error) {
    // this may be called after the query is deregistered, because shutdown is ansynchronous and
    // may time out. when ths happens, the shutdown thread in streams may call this method.
    final PerQueryListener listener = perQuery.get(query.getQueryId());
    if (listener != null) {
      listener.onError(error);
      listener.setKsqlQueryState(query.getQueryStatus().toString());
    }
  }

  @Override
  public void onDeregister(final QueryMetadata query) {
    perQuery.get(query.getQueryId()).onDeregister();
    perQuery.remove(query.getQueryId());
  }

  private static final String NO_ERROR = "NO_ERROR";
  private static final int DEFAULT_VAL = -1;

  private static class PerQueryListener {
    private final Metrics metrics;
    private final MetricName stateMetricName;
    private final MetricName errorMetricName;
    private final MetricName queryRestartMetricName;
    private final MetricName ksqlQueryStatusMetricName;
    private final MetricName stateNumMetricName;
    private final MetricName errorNumMetricName;
    private final MetricName ksqlQueryStatusNumMetricName;
    private final CumulativeSum queryRestartSum;
    private final Ticker ticker;

    private volatile String state = "-";
    private volatile String ksqlQueryState = "-";
    private volatile String error = NO_ERROR;

    PerQueryListener(
        final Metrics metrics,
        final String groupPrefix,
        final String queryId,
        final Map<String, String> metricsTags
    ) {
      this(metrics, groupPrefix, queryId, CURRENT_TIME_MILLIS_TICKER, metricsTags);
    }

    PerQueryListener(
        final Metrics metrics,
        final String groupPrefix,
        final String queryId,
        final Ticker ticker,
        final Map<String, String> metricsTags
    ) {
      Objects.requireNonNull(groupPrefix, "groupPrefix");
      Objects.requireNonNull(queryId, "queryId");
      this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null.");
      this.ticker = Objects.requireNonNull(ticker, "ticker");

      final String type = queryId.toLowerCase().contains("transient") ? "transient_" : "query_" ;

      final String tag = "_confluent-ksql-" + groupPrefix + type + queryId;

      final Map<String, String> tagsForStateAndError = new HashMap<>(metricsTags);
      tagsForStateAndError.put("status", tag);
      this.stateMetricName = metrics.metricName(
          "query-status",
          groupPrefix + "ksql-queries",
          "The current Kafka Streams status of the given query.",
          tagsForStateAndError
      );

      errorMetricName = metrics.metricName(
          "error-status",
          groupPrefix + "ksql-queries",
          "The current error status of the given query, if the state is in ERROR state",
          tagsForStateAndError
      );

      final Map<String, String> restartTags = new HashMap<>(tagsForStateAndError);
      restartTags.put("query-id", queryId);
      queryRestartMetricName = metrics.metricName(
          QUERY_RESTART_METRIC_NAME,
          groupPrefix + "ksql-queries",
          QUERY_RESTART_METRIC_DESCRIPTION,
          restartTags
      );

      ksqlQueryStatusMetricName = metrics.metricName(
              "ksql-query-status",
              groupPrefix + "ksql-queries",
              "The current ksqlDB status of the given query.",
              tagsForStateAndError
      );

      this.queryRestartSum = new CumulativeSum();
      this.metrics.addMetric(stateMetricName, (Gauge<String>) (config, now) -> state);
      this.metrics.addMetric(errorMetricName, (Gauge<String>) (config, now) -> error);
      this.metrics.addMetric(queryRestartMetricName, queryRestartSum);
      this.metrics.addMetric(ksqlQueryStatusMetricName,
              (Gauge<String>) (config, now) -> ksqlQueryState);

      stateNumMetricName = metrics.metricName(
              "query-status-num",
              groupPrefix + "ksql-queries",
              "The current Kafka Streams status number of the given query.",
              tagsForStateAndError
      );
      errorNumMetricName = metrics.metricName(
              "error-status-num",
              groupPrefix + "ksql-queries",
              "The current error status number of the given query, if the state is in ERROR state",
              tagsForStateAndError
      );
      ksqlQueryStatusNumMetricName = metrics.metricName(
              "ksql-query-status-num",
              groupPrefix + "ksql-queries",
              "The current ksqlDB status number of the given query.",
              tagsForStateAndError
      );

      this.metrics.addMetric(stateNumMetricName,
              (Gauge<Integer>) (config, now) -> getValue(State.class, state));
      this.metrics.addMetric(errorNumMetricName,
              (Gauge<Integer>) (config, now) -> getValue(QueryError.Type.class, error));
      this.metrics.addMetric(ksqlQueryStatusNumMetricName,
              (Gauge<Integer>) (config, now) -> getValue(KsqlQueryStatus.class, ksqlQueryState));
    }

    public void onChange(final State newState, final State oldState) {
      state = newState.toString();

      if (newState != State.ERROR) {
        error = NO_ERROR;
      }
    }

    public void setKsqlQueryState(final String ksqlQueryState) {
      this.ksqlQueryState = ksqlQueryState;
    }

    public void onError(final QueryError observedError) {
      error = observedError.getType().name();
      queryRestartSum.record(new MetricConfig(), 1, System.currentTimeMillis());
    }

    public void onDeregister() {
      metrics.removeMetric(stateMetricName);
      metrics.removeMetric(errorMetricName);
      metrics.removeMetric(queryRestartMetricName);
      metrics.removeMetric(ksqlQueryStatusMetricName);
      metrics.removeMetric(stateNumMetricName);
      metrics.removeMetric(errorNumMetricName);
      metrics.removeMetric(ksqlQueryStatusNumMetricName);
    }

    private <T extends Enum<T>> int getValue(final Class<T> enumClass, final String str) {
      for (T enumConstant : enumClass.getEnumConstants()) {
        if (enumConstant.name().equals(str.toUpperCase())) {
          return enumConstant.ordinal();
        }
      }
      return DEFAULT_VAL;
    }
  }
}
