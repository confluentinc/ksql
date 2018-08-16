/**
 * Copyright 2018 Confluent Inc.
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

import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.Topology;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class QueryMetadataTest {

  private final String statementString = "foo";
  private final OutputNode outputNode = EasyMock.niceMock(OutputNode.class);
  private final String executionPlan = "bar";
  private final DataSource.DataSourceType dataSourceType = DataSourceType.KSTREAM;
  private final String queryApplicationId = "Query1";
  private final KafkaTopicClient kafkaTopicClient = EasyMock.niceMock(KafkaTopicClient.class);
  private final Topology topoplogy = EasyMock.niceMock(Topology.class);
  private final Map<String, Object> overriddenProperties = Collections.emptyMap();


  @Test
  public void shouldReturnCorrectStatusForCreated() {

    final double value = getValue(State.CREATED);
    assertThat(value, equalTo(0.0));
  }

  @Test
  public void shouldReturnCorrectStatusForRebalancing() {
    final double value = getValue(State.REBALANCING);
    assertThat(value, equalTo(1.0));
  }


  @Test
  public void shouldReturnCorrectStatusForRunning() {
    final double value = getValue(State.RUNNING);
    assertThat(value, equalTo(2.0));
  }


  @Test
  public void shouldReturnCorrectStatusForPendingShutdown() {
    final double value = getValue(State.PENDING_SHUTDOWN);
    assertThat(value, equalTo(3.0));
  }

  @Test
  public void shouldReturnCorrectStatusForNotRunning() {
    final double value = getValue(State.NOT_RUNNING);
    assertThat(value, equalTo(4.0));
  }

  @Test
  public void shouldReturnCorrectStatusForError() {
    final double value = getValue(State.ERROR);
    assertThat(value, equalTo(5.0));
  }

  @Test
  public void shouldCleanUpMetrics() {
    final double value = addAndRemoveMetric(State.ERROR);
    assertThat(value, equalTo(5.0));
  }

  private double addAndRemoveMetric(final State state) {
    final QueryMetadata queryMetadata = getQueryMetadata(state);
    final Metrics metrics = MetricsTestUtil.getMetrics();
    queryMetadata.start(metrics);
    final Double value = Double.valueOf(metrics.metric(metrics.metricName(queryApplicationId + "-query-status", "ksql-queries")).metricValue().toString());
    assertNotNull(metrics.getSensor(queryMetadata.getSensorList().get(0).name()));
    queryMetadata.close(metrics);
    assertNull(metrics.getSensor(queryMetadata.getSensorList().get(0).name()));
    return value;
  }

  private QueryMetadata getQueryMetadata(final State state) {
    final KafkaStreams kafkaStreams = EasyMock.niceMock(KafkaStreams.class);
    EasyMock.expect(kafkaStreams.state()).andReturn(state).once();
    EasyMock.replay(kafkaStreams);
    return new QueryMetadata(
        statementString,
        kafkaStreams,
        outputNode,
        executionPlan,
        dataSourceType,
        queryApplicationId,
        kafkaTopicClient,
        topoplogy,
        overriddenProperties
    );
  }

  private double getValue(final State state) {
    final QueryMetadata queryMetadata = getQueryMetadata(state);
    final Metrics metrics = MetricsTestUtil.getMetrics();
    queryMetadata.start(metrics);
    return Double.valueOf(metrics.metric(metrics.metricName(queryApplicationId + "-query-status", "ksql-queries")).metricValue().toString());
  }

}
