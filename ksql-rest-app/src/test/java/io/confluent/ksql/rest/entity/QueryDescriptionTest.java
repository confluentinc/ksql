package io.confluent.ksql.rest.entity;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class QueryDescriptionTest {
  private static Schema SCHEMA =
      SchemaBuilder.struct()
          .field("field1", SchemaBuilder.int32().build())
          .field("field2", SchemaBuilder.string().build())
          .build();
  private static String STATEMENT = "statement";

  private static class FakeSourceNode extends StructuredDataSourceNode {
    FakeSourceNode(String name) {
      super(
          new PlanNodeId("fake"),
          new KsqlStream(
              STATEMENT, name, SCHEMA, SCHEMA.fields().get(0),
              new MetadataTimestampExtractionPolicy(),
              new KsqlTopic(name, name, new KsqlJsonTopicSerDe())),
          SCHEMA);
    }
  }

  private static class FakeOutputNode extends OutputNode {
    FakeOutputNode(FakeSourceNode sourceNode) {
      super(
          new PlanNodeId("fake"), sourceNode, SCHEMA, Optional.of(new Integer(1)),
          new MetadataTimestampExtractionPolicy());
    }

    @Override
    public Field getKeyField() {
      return null;
    }

    @Override
    public SchemaKStream buildStream(
        StreamsBuilder builder, KsqlConfig ksqlConfig, KafkaTopicClient kafkaTopicClient,
        FunctionRegistry functionRegistry, Map<String, Object> props,
        SchemaRegistryClient schemaRegistryClient) {
      return null;
    }
  }

  @Test
  public void shouldSetFieldsCorrectlyForQueryMetadata() {
    KafkaStreams queryStreams = niceMock(KafkaStreams.class);
    FakeSourceNode sourceNode = new FakeSourceNode("source");
    OutputNode outputNode = new FakeOutputNode(sourceNode);
    Topology topology = mock(Topology.class);
    TopologyDescription topologyDescription = mock(TopologyDescription.class);
    expect(topology.describe()).andReturn(topologyDescription);
    replay(queryStreams, topology, topologyDescription);
    Map<String, Object> streamsProperties = Collections.singletonMap("k", "v");
    QueryMetadata queryMetadata = new QueuedQueryMetadata(
        "test statement", queryStreams, outputNode, "execution plan",
        new LinkedBlockingQueue<>(), DataSource.DataSourceType.KSTREAM, "app id",
        null, topology, streamsProperties);

    QueryDescription queryDescription = QueryDescription.forQueryMetadata(queryMetadata);

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
    KafkaStreams queryStreams = niceMock(KafkaStreams.class);
    FakeSourceNode sourceNode = new FakeSourceNode("source");
    OutputNode outputNode = new FakeOutputNode(sourceNode);
    Topology topology = mock(Topology.class);
    TopologyDescription topologyDescription = mock(TopologyDescription.class);
    expect(topology.describe()).andReturn(topologyDescription);
    replay(topology, topologyDescription);
    KsqlTopic sinkTopic = new KsqlTopic("fake_sink", "fake_sink", new KsqlJsonTopicSerDe());
    KsqlStream fakeSink = new KsqlStream(
        STATEMENT, "fake_sink", SCHEMA, SCHEMA.fields().get(0),
        new MetadataTimestampExtractionPolicy(), sinkTopic);
    Map<String, Object> streamsProperties = Collections.singletonMap("k", "v");

    PersistentQueryMetadata queryMetadata = new PersistentQueryMetadata(
        "test statement", queryStreams, outputNode, fakeSink,"execution plan",
        new QueryId("query_id"), DataSource.DataSourceType.KSTREAM, "app id", null,
        sinkTopic, topology, streamsProperties);
    QueryDescription queryDescription = QueryDescription.forQueryMetadata(queryMetadata);
    assertThat(queryDescription.getId().getId(), equalTo("query_id"));
    assertThat(queryDescription.getSinks(), equalTo(Collections.singleton("fake_sink")));
  }
}
