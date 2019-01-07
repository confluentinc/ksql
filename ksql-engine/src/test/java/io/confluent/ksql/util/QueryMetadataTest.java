/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.internal.QueryStateListener;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import java.util.Collections;
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
public class QueryMetadataTest {

  private static final String QUERY_APPLICATION_ID = "Query1";

  @Mock
  private OutputNode outputNode;
  @Mock
  private Topology topoplogy;
  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private QueryStateListener listener;
  @Mock
  private Consumer<QueryMetadata> closeCallback;
  private QueryMetadata query;

  @Before
  public void setup() {
    query = new QueryMetadata(
        "foo",
        kafkaStreams,
        outputNode,
        "bar",
        DataSourceType.KSTREAM,
        QUERY_APPLICATION_ID,
        topoplogy,
        Collections.emptyMap(),
        closeCallback
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

  @Test
  public void shouldCloseKStreamsAppOnCloseThenCloseCallback() {
    // When:
    query.close();

    // Then:
    final InOrder inOrder = inOrder(kafkaStreams, closeCallback);
    inOrder.verify(kafkaStreams).close();
    inOrder.verify(closeCallback).accept(query);
  }

  @Test
  public void shouldCleanUpKStreamsAppAfterCloseOnClose() {
    // When:
    query.close();

    // Then:
    final InOrder inOrder = inOrder(kafkaStreams);
    inOrder.verify(kafkaStreams).close();
    inOrder.verify(kafkaStreams).cleanUp();
  }
}
