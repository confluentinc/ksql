/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metrics.KsqlMetrics;
import io.confluent.ksql.metrics.QueryMetric;
import io.confluent.ksql.planner.plan.OutputNode;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class QueryMetadata {
  private final String statementString;
  private final KafkaStreams kafkaStreams;
  private final OutputNode outputNode;
  private final String executionPlan;
  private final DataSource.DataSourceType dataSourceType;
  private final String queryDescription;

  private final QueryMetric queryMetric;
  private final MetricName startTimeMetric;
  private final MetricName runningTimeMetric;
  private final MetricName executedTimeMetric;

  private long startTime;
  private boolean started;

  private static final String metricGroupName = "query-metrics";

  public QueryMetadata(String statementString,
                       KafkaStreams kafkaStreams,
                       OutputNode outputNode,
                       String executionPlan,
                       DataSource.DataSourceType dataSourceType,
                       KsqlMetrics ksqlMetrics,
                       String queryDescription) {
    this.statementString = statementString;
    this.kafkaStreams = kafkaStreams;
    this.outputNode = outputNode;
    this.executionPlan = executionPlan;
    this.dataSourceType = dataSourceType;
    this.queryDescription = queryDescription;

    this.queryMetric = ksqlMetrics.addQueryMetric(queryDescription);
    this.startTimeMetric = queryMetric.metricName("start-timestamp", metricGroupName, "Start time in millis");
    this.runningTimeMetric = queryMetric.metricName("running-seconds", metricGroupName, "Running time in seconds");
    this.executedTimeMetric = queryMetric.metricName("executed-seconds", metricGroupName, "Execute time in seconds");
  }

  /**
   * Start the Compiled Query (Kafka Streams)
   */
  public void start() {
    if (started) {
      throw new KsqlException(queryDescription + " already started!");
    }
    kafkaStreams.start();
    started = true;
    startTime = System.currentTimeMillis();
    if (queryMetric != null) {
      addStartTimeMetric();
      addRunningTimeMetric();
    }
  }

  private void addStartTimeMetric() {
    queryMetric.addMeasurableMetric(
        startTimeMetric,
        new Measurable() {
          public double measure(MetricConfig config, long now) {
            return startTime;
          }
        }
    );
  }

  private void addRunningTimeMetric() {
    queryMetric.addMeasurableMetric(
        runningTimeMetric,
        new Measurable() {
          public double measure(MetricConfig config, long now) {
            return (System.currentTimeMillis() - startTime) / 1000;
          }
        }
    );
  }

  private void addFinishTimeMetric() {
    long finishTime = (System.currentTimeMillis() - startTime) / 1000;
    queryMetric.addMeasurableMetric(
        executedTimeMetric,
        new Measurable() {
          public double measure(MetricConfig config, long now) {
            return finishTime;
          }
        }
    );
  }

  public void closeAndCleanUp() {
    closeAndCleanUp(0, TimeUnit.SECONDS);
  }

  public void closeAndCleanUp(long timeout, TimeUnit timeUnit) {
    if (started) {
      kafkaStreams.close(timeout, timeUnit);
      kafkaStreams.cleanUp();
      queryMetric.removeMetric(runningTimeMetric);
      addFinishTimeMetric();
      started = false;
    }
  }

  public void setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler streamsExceptionHandler) {
    kafkaStreams.setUncaughtExceptionHandler(streamsExceptionHandler);
  }

  public String getStatementString() {
    return statementString;
  }

  public OutputNode getOutputNode() {
    return outputNode;
  }

  public String getExecutionPlan() {
    return executionPlan;
  }

  public DataSource.DataSourceType getDataSourceType() {
    return dataSourceType;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof QueryMetadata)) {
      return false;
    }

    QueryMetadata that = (QueryMetadata) o;

    return Objects.equals(this.statementString, that.statementString)
        && Objects.equals(this.kafkaStreams, that.kafkaStreams)
        && Objects.equals(this.outputNode, that.outputNode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kafkaStreams, outputNode);
  }
}
