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

package io.confluent.ksql.planner.plan;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;

public class ProjectNodeTest {

  private final PlanNode source = EasyMock.createMock(PlanNode.class);
  private final SchemaKStream stream = EasyMock.createNiceMock(SchemaKStream.class);
  private final StreamsBuilder builder = new StreamsBuilder();
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final FakeKafkaTopicClient kafkaTopicClient = new FakeKafkaTopicClient();
  private final FunctionRegistry functionRegistry = new FunctionRegistry();
  private final HashMap<String, Object> props = new HashMap<>();

  @Test(expected = KsqlException.class)
  public void shouldThrowKsqlExcptionIfSchemaSizeDoesntMatchProjection() {
    mockSourceNode();

    EasyMock.replay(source, stream);

    final ProjectNode node = new ProjectNode(new PlanNodeId("1"),
        source,
        SchemaBuilder.struct()
            .field("field1", Schema.STRING_SCHEMA)
            .field("field2", Schema.STRING_SCHEMA)
            .build(),
        Collections.singletonList(new BooleanLiteral("true")));


    node.buildStream(builder,
        ksqlConfig,
        kafkaTopicClient,
        functionRegistry,
        props, new MockSchemaRegistryClient());
  }

  @Test
  public void shouldCreateProjectionWithFieldNameExpressionPairs() {
    mockSourceNode();
    final BooleanLiteral trueExpression = new BooleanLiteral("true");
    final BooleanLiteral falseExpression = new BooleanLiteral("false");
    EasyMock.expect(stream.select(
        Arrays.asList(new Pair<>("field1", trueExpression),
            new Pair<>("field2", falseExpression))))
        .andReturn(stream);

    EasyMock.replay(source, stream);

    final ProjectNode node = new ProjectNode(new PlanNodeId("1"),
        source,
        SchemaBuilder.struct()
            .field("field1", Schema.STRING_SCHEMA)
            .field("field2", Schema.STRING_SCHEMA)
            .build(),
        Arrays.asList(trueExpression, falseExpression));

    node.buildStream(builder,
        ksqlConfig,
        kafkaTopicClient,
        functionRegistry,
        props, new MockSchemaRegistryClient());

    EasyMock.verify(stream);
  }

  private void mockSourceNode() {
    EasyMock.expect(source.getKeyField()).andReturn(new Field("field1", 0, Schema.STRING_SCHEMA));
    EasyMock.expect(source.buildStream(anyObject(StreamsBuilder.class),
        anyObject(KsqlConfig.class),
        anyObject(KafkaTopicClient.class),
        anyObject(FunctionRegistry.class),
        eq(props), anyObject(SchemaRegistryClient.class))).andReturn(stream);
  }


}