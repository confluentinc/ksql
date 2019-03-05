/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryDescriptionTest {
  private static final Schema SCHEMA =
      SchemaBuilder.struct()
          .field("field1", SchemaBuilder.int32().build())
          .field("field2", SchemaBuilder.string().build())
          .build();
  private static final String STATEMENT = "statement";

  @Mock
  private Consumer<QueryMetadata> queryCloseCallback;

  private static class FakeSourceNode extends StructuredDataSourceNode {
    FakeSourceNode(final String name) {
      super(
          new PlanNodeId("fake"),
          new KsqlStream<>(
              STATEMENT, name, SCHEMA, SCHEMA.fields().get(0),
              new MetadataTimestampExtractionPolicy(),
              new KsqlTopic(name, name, new KsqlJsonTopicSerDe(), false), Serdes.String()),
          SCHEMA);
    }
  }

  private static class FakeOutputNode extends OutputNode {
    FakeOutputNode(final FakeSourceNode sourceNode) {
      super(
          new PlanNodeId("fake"), sourceNode, SCHEMA, Optional.of(1),
          new MetadataTimestampExtractionPolicy());
    }

    @Override
    public Field getKeyField() {
      return null;
    }

    @Override
    public SchemaKStream<?> buildStream(
        final StreamsBuilder builder,
        final KsqlConfig ksqlConfig,
        final ServiceContext serviceContext,
        final ProcessingLogContext processingLogContext,
        final FunctionRegistry functionRegistry,
        final QueryId queryId
    ) {
      return null;
    }

    @Override
    public QueryId getQueryId(final QueryIdGenerator queryIdGenerator) {
      return new QueryId("fake");
    }
  }

  @Test
  public void shouldSetFieldsCorrectlyForQueryMetadata() {
    final KafkaStreams queryStreams = niceMock(KafkaStreams.class);
    final FakeSourceNode sourceNode = new FakeSourceNode("source");
    final OutputNode outputNode = new FakeOutputNode(sourceNode);
    final Topology topology = mock(Topology.class);
    final TopologyDescription topologyDescription = mock(TopologyDescription.class);
    expect(topology.describe()).andReturn(topologyDescription);
    replay(queryStreams, topology, topologyDescription);
    final Map<String, Object> streamsProperties = Collections.singletonMap("k", "v");
    final QueryMetadata queryMetadata = new QueuedQueryMetadata(
        "test statement",
        queryStreams,
        outputNode,
        "execution plan",
        new LinkedBlockingQueue<>(),
        DataSource.DataSourceType.KSTREAM,
        "app id",
        topology,
        streamsProperties,
        streamsProperties,
        queryCloseCallback);

    final QueryDescription queryDescription = QueryDescription.forQueryMetadata(queryMetadata);

    assertThat(queryDescription.getId().getId(), equalTo(""));
    assertThat(queryDescription.getExecutionPlan(), equalTo("execution plan"));
    assertThat(queryDescription.getSources(), equalTo(Collections.singleton("source")));
    assertThat(queryDescription.getStatementText(), equalTo("test statement"));
    assertThat(queryDescription.getTopology(), equalTo(topologyDescription.toString()));
    assertThat(
        queryDescription.getFields(),
        equalTo(
            Arrays.asList(
                new FieldInfo("field1", new SchemaInfo(SchemaInfo.Type.INTEGER, null, null)),
                new FieldInfo("field2", new SchemaInfo(SchemaInfo.Type.STRING, null, null)))));
  }

  @Test
  public void shouldSetFieldsCorrectlyForPersistentQueryMetadata() {
    final KafkaStreams queryStreams = niceMock(KafkaStreams.class);
    final FakeSourceNode sourceNode = new FakeSourceNode("source");
    final OutputNode outputNode = new FakeOutputNode(sourceNode);
    final Topology topology = mock(Topology.class);
    final TopologyDescription topologyDescription = mock(TopologyDescription.class);
    expect(topology.describe()).andReturn(topologyDescription);
    replay(topology, topologyDescription);
    final KsqlTopic sinkTopic = new KsqlTopic("fake_sink", "fake_sink", new KsqlJsonTopicSerDe(), true);
    final KsqlStream fakeSink = new KsqlStream<>(
        STATEMENT, "fake_sink", SCHEMA, SCHEMA.fields().get(0),
        new MetadataTimestampExtractionPolicy(), sinkTopic, Serdes.String());
    final Map<String, Object> streamsProperties = Collections.singletonMap("k", "v");

    final PersistentQueryMetadata queryMetadata = new PersistentQueryMetadata(
        "test statement",
        queryStreams,
        outputNode,
        fakeSink,
        "execution plan",
        new QueryId("query_id"),
        DataSource.DataSourceType.KSTREAM,
        "app id",
        sinkTopic,
        topology,
        streamsProperties,
        streamsProperties,
        queryCloseCallback);
    final QueryDescription queryDescription = QueryDescription.forQueryMetadata(queryMetadata);
    assertThat(queryDescription.getId().getId(), equalTo("query_id"));
    assertThat(queryDescription.getSinks(), equalTo(Collections.singleton("fake_sink")));
  }
}
