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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.internal.QueryStateListener;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import java.util.Collections;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryMetadataTest {

  private static final String QUERY_APPLICATION_ID = "Query1";

  @Mock
  private OutputNode outputNode;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private Topology topoplogy;
  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private QueryStateListener listener;
  private Metrics metrics;
  private MetricName metricName;
  private QueryMetadata query;

  @Before
  public void setup() {
    metrics = MetricsTestUtil.getMetrics();
    metricName = metrics.metricName("query-status", "ksql-queries",
        "The current status of the given query.",
        Collections.singletonMap("status", QUERY_APPLICATION_ID));

    query = new QueryMetadata(
        "foo",
        kafkaStreams,
        outputNode,
        "bar",
        DataSourceType.KSTREAM,
        QUERY_APPLICATION_ID,
        kafkaTopicClient,
        topoplogy,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldSetInitialStateWhenListenerAdd() {
    // Given:
    when(kafkaStreams.state()).thenReturn(State.CREATED);

    // When:
    query.registerQueryStateListener(listener);

    // Then:
    verify(listener).onChange(State.CREATED, State.CREATED);
  }

  @Test
  public void shouldConnectAnyListenerToStreamAppOnStart() {
    // Given:
    query.registerQueryStateListener(listener);

    // When:
    query.start();

    // Then:
    verify(kafkaStreams).setStateListener(listener);
  }

  @Test
  public void shouldCloseAnyListenerOnClose() {
    // Given:
    query.registerQueryStateListener(listener);

    // When:
    query.close();

    // Then:
    verify(listener).close();
  }

  @Test
  public void shouldReturnStreamState() {
    // Given:
    when(kafkaStreams.state()).thenReturn(State.PENDING_SHUTDOWN);

    // When:
    final String state = query.getState();

    // Then:
    assertThat(state, is("PENDING_SHUTDOWN"));
  }
}
