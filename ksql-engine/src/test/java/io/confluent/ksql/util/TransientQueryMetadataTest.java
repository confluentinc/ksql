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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TransientQueryMetadataTest {

  private static final String QUERY_ID = "queryId";
  private static final String EXECUTION_PLAN = "execution plan";
  private static final String SQL = "sql";
  private static final long CLOSE_TIMEOUT = 10L;

  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private LogicalSchema logicalSchema;
  @Mock
  private Set<SourceName> sourceNames;
  @Mock
  private BlockingRowQueue rowQueue;
  @Mock
  private Topology topology;
  @Mock
  private Map<String, Object> props;
  @Mock
  private Map<String, Object> overrides;
  @Mock
  private Consumer<QueryMetadata> closeCallback;
  private TransientQueryMetadata query;

  @Before
  public void setUp()  {
    query = new TransientQueryMetadata(
        SQL,
        kafkaStreams,
        logicalSchema,
        sourceNames,
        EXECUTION_PLAN,
        rowQueue,
        QUERY_ID,
        topology,
        props,
        overrides,
        closeCallback,
        CLOSE_TIMEOUT
    );
  }

  @Test
  public void shouldCloseQueueBeforeTopologyToAvoidDeadLock() {
    // When:
    query.close();

    // Then:
    final InOrder inOrder = inOrder(rowQueue, kafkaStreams);
    inOrder.verify(rowQueue).close();
    inOrder.verify(kafkaStreams).close(any());
  }

  @Test
  public void shouldCallCloseOnStop() {
    // When:
    query.stop();

    // Then:
    final InOrder inOrder = inOrder(rowQueue, kafkaStreams, closeCallback);
    inOrder.verify(rowQueue).close();
    inOrder.verify(kafkaStreams).close(any());
    inOrder.verify(kafkaStreams).cleanUp();
    inOrder.verify(closeCallback).accept(query);
  }
}