/*
 * Copyright 2021 Confluent Inc.
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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageUtilizationMetricsReporter implements MetricsReporter {
  private static final Logger LOGGER
      = LoggerFactory.getLogger(StorageUtilizationMetricsReporter.class);
  private static final String METRIC_GROUP = "ksqldb_utilization";
  private static final String TASK_STORAGE_USED_BYTES = "task_storage_used_bytes";
  private static final Pattern NAMED_TOPOLOGY_PATTERN = Pattern.compile("(.*?)__\\d*_\\d*");
  private static final Pattern QUERY_ID_PATTERN =
      Pattern.compile("(?<=query_|transient_)(.*?)(?=-)");

  private Map<String, Map<String, TaskStorageMetric>> metricsSeen;
  private Metrics metricRegistry;
  private static Map<String, String> customTags = new HashMap<>();
  private static AtomicInteger numberStatefulTasks = new AtomicInteger(0);

  public StorageUtilizationMetricsReporter() {
  }

  @Override
  public void init(final List<KafkaMetric> list) {
  }

  @Override
  public synchronized void configure(final Map<String, ?> map) {
    this.metricRegistry = (Metrics) requireNonNull(
        map.get(KsqlConfig.KSQL_INTERNAL_METRICS_CONFIG)
    );
    this.metricsSeen = new HashMap<>();
  }

  public static void configureShared(
      final File baseDir, 
      final Metrics metricRegistry, 
      final Map<String, String> configTags
  ) {
    customTags = ImmutableMap.copyOf(configTags);
    LOGGER.info("Adding node level storage usage gauges");
    final MetricName nodeAvailable =
        metricRegistry.metricName("node_storage_free_bytes", METRIC_GROUP, customTags);
    final MetricName nodeTotal =
        metricRegistry.metricName("node_storage_total_bytes", METRIC_GROUP, customTags);
    final MetricName nodeUsed =
        metricRegistry.metricName("node_storage_used_bytes", METRIC_GROUP, customTags);
    final MetricName nodePct =
        metricRegistry.metricName("storage_utilization", METRIC_GROUP, customTags);
    final MetricName maxTaskPerNode = 
        metricRegistry.metricName("max_used_task_storage_bytes", METRIC_GROUP, customTags);
    final MetricName numStatefulTasks =
        metricRegistry.metricName("num_stateful_tasks", METRIC_GROUP, customTags);
    metricRegistry.addMetric(
        nodeAvailable,
        (Gauge<Long>) (config, now) -> baseDir.getFreeSpace()
    );
    metricRegistry.addMetric(
        nodeTotal,
        (Gauge<Long>) (config, now) -> baseDir.getTotalSpace()
    );
    metricRegistry.addMetric(
        nodeUsed,
        (Gauge<Long>) (config, now) -> (baseDir.getTotalSpace() - baseDir.getFreeSpace())
    );
    metricRegistry.addMetric(
        nodePct,
        (Gauge<Double>) (config, now) -> 
        (((double) baseDir.getTotalSpace() - (double) baseDir.getFreeSpace()) 
            / (double) baseDir.getTotalSpace())
    );
    metricRegistry.addMetric(
        maxTaskPerNode,
        (Gauge<BigInteger>) (config, now) -> (getMaxTaskUsage(metricRegistry))
    );
    metricRegistry.addMetric(
        numStatefulTasks,
        (Gauge<Integer>) (config, now) -> (numberStatefulTasks.get())
    );
  }

  @Override
  public void metricChange(final KafkaMetric metric) {
    if (!metric.metricName().name().equals("total-sst-files-size")) {
      return;
    }

    handleNewSstFilesSizeMetric(
        metric,
        metric.metricName().tags().getOrDefault("task-id", ""),
        getQueryId(metric)
    );
  }

  @Override
  public void metricRemoval(final KafkaMetric metric) {
    final MetricName metricName = metric.metricName();
    if (!metricName.name().equals("total-sst-files-size")) {
      return;
    }

    final String queryId = getQueryId(metric);
    final String taskId = metric.metricName().tags().getOrDefault("task-id", "");
    final TaskStorageMetric taskMetric = metricsSeen.get(queryId).get(taskId);

    handleRemovedSstFileSizeMetric(taskMetric, metric, queryId, taskId);
  }

  @Override
  public void close() {
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return null;
  }

  @Override
  public void validateReconfiguration(final Map<String, ?> configs) throws ConfigException {

  }

  @Override
  public void reconfigure(final Map<String, ?> configs) {

  }

  @Override
  public void contextChange(final MetricsContext metricsContext) {

  }

  private synchronized void handleNewSstFilesSizeMetric(
      final KafkaMetric metric,
      final String taskId,
      final String queryId
  ) {
    final Map<String, String> queryMetricTags = getQueryMetricTags(queryId);
    final Map<String, String> taskMetricTags = getTaskMetricTags(queryMetricTags, taskId);
    LOGGER.debug("Updating disk usage metrics");
    // if we haven't seen a task for this query yet
    if (!metricsSeen.containsKey(queryId)) {
      metricRegistry.addMetric(
          metricRegistry.metricName(
          "query_storage_used_bytes",
          METRIC_GROUP,
          queryMetricTags),
          (Gauge<BigInteger>) (config, now) -> computeQueryMetric(queryId)
      );
      metricsSeen.put(queryId, new HashMap<>());
    }
    final TaskStorageMetric newMetric;
    // We haven't seen a metric for this query's task before
    if (!metricsSeen.get(queryId).containsKey(taskId)) {
      numberStatefulTasks.getAndIncrement();
      // create a new task level metric to track state store storage usage
      newMetric = new TaskStorageMetric(
        metricRegistry.metricName(
          TASK_STORAGE_USED_BYTES,
          METRIC_GROUP,
          taskMetricTags
        ));
      // add to list of seen task metrics for this query
      metricsSeen.get(queryId).put(taskId, newMetric);
      // create gauge for task level storage usage
      metricRegistry.addMetric(
          newMetric.metricName,
          (Gauge<BigInteger>) (config, now) -> newMetric.getValue()
      );
    } else {
      // We have this metric already
      newMetric = metricsSeen.get(queryId).get(taskId);
    }
    newMetric.add(metric);
  }

  private synchronized void handleRemovedSstFileSizeMetric(
      final TaskStorageMetric taskMetric,
      final KafkaMetric metric,
      final String queryId,
      final String taskId
  ) {
    // remove storage metric for this task
    taskMetric.remove(metric);
    numberStatefulTasks.getAndDecrement();
    if (taskMetric.metrics.size() == 0) {
      // no more storage metrics for this task, can remove task gauge
      metricRegistry.removeMetric(taskMetric.metricName);
      metricsSeen.get(queryId).remove(taskId);
      if (metricsSeen.get(queryId).size() == 0) {
        // we've removed all the task metrics for this query, don't need the query metrics anymore
        metricRegistry.removeMetric(metricRegistry.metricName(
            "query_storage_used_bytes",
            METRIC_GROUP,
            getQueryMetricTags(queryId))
        );
      }
    }
  }

  private BigInteger computeQueryMetric(final String queryId) {
    BigInteger queryMetricSum = BigInteger.ZERO;
    for (final Supplier<BigInteger> gauge : getGaugesForQuery(queryId)) {
      queryMetricSum = queryMetricSum.add(gauge.get());
    }
    return queryMetricSum;
  }
  
  public static synchronized BigInteger getMaxTaskUsage(final Metrics metricRegistry) {
    final Collection<KafkaMetric> taskMetrics = metricRegistry
        .metrics()
        .entrySet()
        .stream()
        .filter(e -> e.getKey().name().contains(TASK_STORAGE_USED_BYTES))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        .values();
    final Optional<BigInteger> maxOfTaskMetrics = taskMetrics
        .stream()
        .map(e -> (BigInteger) e.metricValue())
        .reduce(BigInteger::max);
    return maxOfTaskMetrics.orElse(BigInteger.ZERO);
  }

  private synchronized Collection<Supplier<BigInteger>> getGaugesForQuery(final String queryId) {
    return metricsSeen.get(queryId).values().stream()
      .map(v -> (Supplier<BigInteger>) v::getValue)
      .collect(Collectors.toList());
  }

  private String getQueryId(final KafkaMetric metric) {
    final String taskName = metric.metricName().tags().getOrDefault("task-id", "");
    final Matcher namedTopologyMatcher = NAMED_TOPOLOGY_PATTERN.matcher(taskName);
    if (namedTopologyMatcher.find()) {
      return namedTopologyMatcher.group(1);
    }

    final String queryIdTag = metric.metricName().tags().getOrDefault("thread-id", "");
    final Matcher matcher = QUERY_ID_PATTERN.matcher(queryIdTag);
    if (matcher.find()) {
      return matcher.group(1);
    } else {
      throw new KsqlException("Missing query ID when reporting utilization metrics");
    }
  }
  
  private Map<String, String> getQueryMetricTags(final String queryId) {
    final Map<String, String> queryMetricTags = new HashMap<>(customTags);
    queryMetricTags.put("query-id", queryId);
    return ImmutableMap.copyOf(queryMetricTags);
  }

  private Map<String, String> getTaskMetricTags(
      final Map<String, String> queryTags, 
      final String taskId
  ) {
    final Map<String, String> taskMetricTags = new HashMap<>(queryTags);
    taskMetricTags.put("task-id", taskId);
    return ImmutableMap.copyOf((taskMetricTags));
  }
  
  @VisibleForTesting
  static void setTags(final Map<String, String> tags) {
    customTags = tags;
  }

  private static class TaskStorageMetric {
    final MetricName metricName;
    private final Map<MetricName, KafkaMetric> metrics = new ConcurrentHashMap<>();

    TaskStorageMetric(final MetricName metricName) {
      this.metricName = metricName;
    }

    private void add(final KafkaMetric metric) {
      metrics.put(metric.metricName(), metric);
    }

    private void remove(final KafkaMetric metric) {
      metrics.remove(metric.metricName());
    }

    public BigInteger getValue() {
      BigInteger newValue = BigInteger.ZERO;
      for (KafkaMetric metric : metrics.values()) {
        final BigInteger value = (BigInteger) metric.metricValue();
        newValue = newValue.add(value);
      }
      return newValue;
    }
  }

}
