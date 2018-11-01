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

import io.confluent.ksql.internal.QueryStateListener;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.Topology;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(EasyMockRunner.class)
public class QueryMetadataTest {

  private final String statementString = "foo";
  private  OutputNode outputNode;
  private final String executionPlan = "bar";
  private final DataSource.DataSourceType dataSourceType = DataSourceType.KSTREAM;
  private final String queryApplicationId = "Query1";
  private final KafkaTopicClient kafkaTopicClient = EasyMock.niceMock(KafkaTopicClient.class);
  private final Topology topoplogy = EasyMock.niceMock(Topology.class);
  private final Map<String, Object> overriddenProperties = Collections.emptyMap();

  private KafkaStreams kafkaStreams;
  private QueryMetadata queryMetadata;
  private Metrics metrics;
  private MetricName metricName;
  private final String metricGroupName = "ksql-queries";

  @Mock(MockType.NICE)
  private Consumer<QueryMetadata> onStartQueryEvent;

  @Before
  public void setup() {
    metrics = MetricsTestUtil.getMetrics();
    metricName = metrics.metricName("query-status", metricGroupName,
        "The current status of the given query.",
        Collections.singletonMap("status", queryApplicationId));
    outputNode = EasyMock.niceMock(OutputNode.class);
    kafkaStreams = EasyMock.niceMock(KafkaStreams.class);
    EasyMock.replay(kafkaStreams, outputNode, onStartQueryEvent);

    queryMetadata = new QueryMetadata(
        statementString,
        kafkaStreams,
        outputNode,
        executionPlan,
        dataSourceType,
        queryApplicationId,
        kafkaTopicClient,
        topoplogy,
        overriddenProperties,
        onStartQueryEvent
    );
  }

  @Test
  public void shouldAddAndRemoveTheMetricOnClose() {
    EasyMock.reset(kafkaStreams);
    EasyMock.expect(kafkaStreams.state()).andReturn(State.RUNNING).once();
    EasyMock.expect(kafkaStreams.state()).andReturn(State.NOT_RUNNING).once();
    EasyMock.replay(kafkaStreams);
    final QueryStateListener queryStateListener = new QueryStateListener(metrics, kafkaStreams, queryApplicationId);
    kafkaStreams.setStateListener(EasyMock.eq(queryStateListener));

    queryMetadata.registerQueryStateListener(queryStateListener);
    queryMetadata.start();
    assertThat(metrics.metric(metricName).metricName().name(), equalTo("query-status"));
    assertThat(metrics.metric(metricName).metricValue().toString(), equalTo("RUNNING"));
    queryStateListener.onChange(State.REBALANCING, State.RUNNING);
    assertThat(metrics.metric(metricName).metricValue().toString(), equalTo("REBALANCING"));
    queryStateListener.onChange(State.RUNNING, State.REBALANCING);
    assertThat(metrics.metric(metricName).metricValue().toString(), equalTo("RUNNING"));
    queryMetadata.close();
    EasyMock.verify(kafkaStreams);
    assertThat(metrics.metric(metricName), nullValue());

  }

  @Test
  public void shouldFireEventOnStart() {
    // Given:
    EasyMock.reset(onStartQueryEvent);
    onStartQueryEvent.accept(EasyMock.anyObject(QueryMetadata.class));
    EasyMock.expectLastCall();
    EasyMock.replay(onStartQueryEvent);

    // When:
    queryMetadata.start();

    // Then:
    EasyMock.verify(onStartQueryEvent);
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowIfStartEventThrows() {
    // Given:
    EasyMock.reset(onStartQueryEvent);
    onStartQueryEvent.accept(EasyMock.anyObject(QueryMetadata.class));
    EasyMock.expectLastCall().andThrow(new KsqlException("Boom"));
    EasyMock.replay(onStartQueryEvent);

    // When:
    queryMetadata.start();
  }
}
