/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import io.confluent.ksql.logging.processing.MeteredProcessingLoggerFactory;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.confluent.ksql.util.QueryMetadata.Listener;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TransientQueryMetadataTest {

  private static final String QUERY_APPLICATION_ID = "queryApplicationId";
  private static final String EXECUTION_PLAN = "execution plan";
  private static final String SQL = "sql";
  private static final long CLOSE_TIMEOUT = 10L;

  @Mock
  private KafkaStreamsBuilder kafkaStreamsBuilder;
  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private LogicalSchema logicalSchema;
  @Mock
  private Set<SourceName> sourceNames;
  @Mock
  private BlockingRowQueue rowQueue;
  @Mock
  private QueryId queryId;
  @Mock
  private Topology topology;
  @Mock
  private Map<String, Object> props;
  @Mock
  private Map<String, Object> overrides;
  @Mock
  private Consumer<QueryMetadata> closeCallback;
  @Mock
  private Listener listener;
  @Mock
  private MeteredProcessingLoggerFactory loggerFactory;

  private TransientQueryMetadata query;

  @Before
  public void setUp()  {
    when(kafkaStreamsBuilder.build(any(), any())).thenReturn(kafkaStreams);
    when(kafkaStreams.state()).thenReturn(State.NOT_RUNNING);
    when(sourceNames.toArray()).thenReturn(new SourceName[0]);

    query = new TransientQueryMetadata(
        SQL,
        logicalSchema,
        sourceNames,
        EXECUTION_PLAN,
        rowQueue,
        queryId,
        QUERY_APPLICATION_ID,
        topology,
        kafkaStreamsBuilder,
        props,
        overrides,
        CLOSE_TIMEOUT,
        10,
        ResultType.STREAM,
        0L,
        0L,
        listener,
        loggerFactory
    );
    query.initialize();
  }

  @Test
  public void shouldCloseQueueBeforeTopologyToAvoidDeadLock() {
    // Given:
    query.start();

    // When:
    query.close();

    // Then:
    final InOrder inOrder = inOrder(rowQueue, kafkaStreams);
    inOrder.verify(rowQueue).close();
    inOrder.verify(kafkaStreams).close(any(java.time.Duration.class));
  }

  @Test
  public void shouldReturnPushQueryTypeByDefault() {
    assertThat(query.getQueryType(), is(KsqlQueryType.PUSH));
  }
}