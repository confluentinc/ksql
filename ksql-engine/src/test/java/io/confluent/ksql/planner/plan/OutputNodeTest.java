/*
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
 */

package io.confluent.ksql.planner.plan;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import junit.framework.AssertionFailedError;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsBuilder;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Test;
import org.junit.runner.RunWith;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@RunWith(EasyMockRunner.class)
public class OutputNodeTest {
  @Mock(MockType.NICE)
  private Schema schema;
  @Mock(MockType.NICE)
  private PlanNodeId id;
  @Mock(MockType.NICE)
  private PlanNode source;
  @Mock(MockType.NICE)
  private TimestampExtractionPolicy timestampExtractionPolicy;
  @Mock
  private OutputNode.LimitHandler limitHandler;
  private OutputNode node;
  private OutputNode.Callback callback;

  @Test
  public void shouldNotBlowUpIfNoLimitHandlerRegistered() {
    // Given:
    node = new TestOutputNode(id, source, schema, Optional.of(1), timestampExtractionPolicy);
    callback = node.getCallback();

    // When:
    callback.shouldQueue();
    callback.onQueued();

    // Then:
    // No exception thrown.
  }

  @Test
  public void shouldNotCallLimitHandlerWhenNoLimit() {
    // Given:
    givenOutputNodeWithLimit(Optional.empty());

    limitHandler.limitReached();
    expectLastCall().andThrow(new AssertionFailedError()).anyTimes();

    replay(limitHandler);

    // When:
    callback.shouldQueue();
    callback.onQueued();

    // Then:
    verify(limitHandler);
  }

  @Test
  public void shouldQueueUntilLimitReached() {
    // Given:
    givenOutputNodeWithLimit(Optional.of(2));

    // Then:
    assertThat(callback.shouldQueue(), is(true));
    assertThat(callback.shouldQueue(), is(true));
    assertThat(callback.shouldQueue(), is(false));
  }

  @Test
  public void shouldNotCallLimitHandlerIfOnlyPredicateCalled() {
    // Given:
    givenOutputNodeWithLimit(Optional.of(1));

    limitHandler.limitReached();
    expectLastCall().andThrow(new AssertionFailedError()).anyTimes();

    replay(limitHandler);

    // When:
    callback.shouldQueue();
    callback.shouldQueue();

    // Then:
    verify(limitHandler);
  }

  @Test
  public void shouldCallLimitHandlerExactlyOnceWhenLimitReached() {
    // Given:
    givenOutputNodeWithLimit(Optional.of(2));

    limitHandler.limitReached();
    expectLastCall().once();

    replay(limitHandler);

    // When:
    callback.onQueued();
    callback.onQueued();
    callback.onQueued();
    callback.onQueued();
    callback.onQueued();

    // Then:
    verify(limitHandler);
  }

  private void givenOutputNodeWithLimit(final Optional<Integer> limit) {
    node = new TestOutputNode(id, source, schema, limit, timestampExtractionPolicy);
    node.setLimitHandler(limitHandler);
    callback = node.getCallback();
  }

  private static class TestOutputNode extends OutputNode {

    private TestOutputNode(final PlanNodeId id,
                           final PlanNode source,
                           final Schema schema,
                           final Optional<Integer> limit,
                           final TimestampExtractionPolicy timestampExtractionPolicy) {
      super(id, source, schema, limit, timestampExtractionPolicy);
    }

    @Override
    public Field getKeyField() {
      return null;
    }

    @Override
    public SchemaKStream buildStream(final StreamsBuilder builder, final KsqlConfig ksqlConfig,
                                     final KafkaTopicClient kafkaTopicClient,
                                     final FunctionRegistry functionRegistry,
                                     final Map<String, Object> props,
                                     final Supplier<SchemaRegistryClient> schemaRegistryClient) {
      return null;
    }
  }
}