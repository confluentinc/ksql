/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test.tools;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.query.QuerySchemas;
import io.confluent.ksql.schema.query.QuerySchemas.SchemaInfo;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.model.PostConditionsNode.PostTopicNode;
import io.confluent.ksql.test.model.SchemaNode;
import io.confluent.ksql.test.model.TestHeader;
import io.confluent.ksql.test.serde.json.ValueSpecJsonSerdeSupplier;
import io.confluent.ksql.test.tools.TestExecutor.TopologyBuilder;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
@RunWith(MockitoJUnitRunner.class)
public class TestExecutorTest {

  private static final String SINK_TOPIC_NAME = "sink_topic";

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
      .build();

  @Mock
  private StubKafkaService kafkaService;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private TestCase testCase;
  @Mock
  private TopologyAndConfigs expectedTopologyAndConfig;
  @Mock
  private TopologyBuilder topologyBuilder;
  @Mock
  private TopologyTestDriver topologyTestDriver;
  @Mock
  private Topic sourceTopic;
  @Mock
  private Topic sinkTopic;
  @Mock
  private PostConditions postConditions;
  @Mock
  private MetaStore metaStore;
  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private TestExecutionListener listener;
  @Mock
  private KeyFormat keyFormat;
  @Mock
  private ValueFormat valueFormat;

  private TestExecutor executor;
  private final Map<SourceName, DataSource> allSources = new HashMap<>();

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    allSources.clear();

    when(serviceContext.getSchemaRegistryClient()).thenReturn(srClient);

    executor = new TestExecutor(
        kafkaService,
        serviceContext,
        ksqlEngine,
        topologyBuilder,
        true
    );

    when(sourceTopic.getName()).thenReturn("source_topic");
    when(topologyTestDriver.createInputTopic(any(), any(), any()))
        .thenReturn(mock(TestInputTopic.class));
    when(sinkTopic.getName()).thenReturn(SINK_TOPIC_NAME);

    final TopologyTestDriverContainer container = TopologyTestDriverContainer.of(
        topologyTestDriver,
        ImmutableList.of(sourceTopic),
        Optional.of(sinkTopic)
    );

    when(topologyTestDriver.producedTopicNames()).thenReturn(ImmutableSet.of(SINK_TOPIC_NAME));

    when(topologyBuilder.buildStreamsTopologyTestDrivers(any(), any(), any(), any(), any(), any()))
        .thenReturn(ImmutableList.of(container));

    when(testCase.getPostConditions()).thenReturn(postConditions);

    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);

    when(metaStore.getAllDataSources()).thenReturn(allSources);

    givenDataSourceTopic(LOGICAL_SCHEMA);
  }

  @Test
  public void shouldNotVerifyTopologyIfNotSet() {
    // Given:
    when(testCase.getExpectedTopology()).thenReturn(Optional.empty());

    // When:
    executor.buildAndExecuteQuery(testCase, listener);

    // Then: did not throw, and
    verify(testCase, never()).getGeneratedTopologies();
  }

  @Test
  public void shouldVerifyTopology() {
    // Given:
    givenExpectedTopology("matching-topology");
    givenActualTopology("matching-topology");

    // When:
    executor.buildAndExecuteQuery(testCase, listener);

    // Then: did not throw, and
    verify(testCase).getExpectedTopology();
    verify(testCase).getGeneratedTopologies();
  }

  @Test
  public void shouldVerifyTopologySchemas() {
    // Given:
    givenExpectedTopology("a-topology", ImmutableMap.of("matching", new SchemaNode(
        LOGICAL_SCHEMA.toString(),
        Optional.of(keyFormat),
        Optional.of(valueFormat)
    )));

    givenActualTopology("a-topology", ImmutableMap.of("matching", new SchemaInfo(
        LOGICAL_SCHEMA,
        Optional.of(keyFormat),
        Optional.of(valueFormat)
    )));

    // When:
    executor.buildAndExecuteQuery(testCase, listener);

    // Then: did not throw, and
    verify(testCase).getGeneratedSchemas();
  }

  @Test
  public void shouldFailOnTopologyMismatch() {
    // Given:
    givenExpectedTopology("expected-topology");
    givenActualTopology("actual-topology");

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> executor.buildAndExecuteQuery(testCase, listener)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Generated topology differs from that built by previous versions of KSQL "
            + "- this likely means there is a non-backwards compatible change.\n"
            + "THIS IS BAD!\n"
            + "Expected: is \"expected-topology\"\n"
            + "     but: was \"actual-topology\""));
  }

  @Test
  public void shouldFailOnLoggerPrefixMismatch() {
    // Given:
    givenExpectedTopology("the-topology", ImmutableMap.of("expected", new SchemaNode(
        LOGICAL_SCHEMA.toString(),
        Optional.of(keyFormat),
        Optional.of(valueFormat)
    )));

    givenActualTopology("the-topology", ImmutableMap.of("actual", new SchemaInfo(
        LOGICAL_SCHEMA,
        Optional.of(keyFormat),
        Optional.of(valueFormat)
    )));

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> executor.buildAndExecuteQuery(testCase, listener)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Schemas used by topology differ from those used by previous versions of KSQL "
            + "- this is likely to mean there is a non-backwards compatible change.\n"
            + "THIS IS BAD!\n"));
  }

  @Test
  public void shouldFailOnSchemasMismatch() {
    // Given:
    givenExpectedTopology("the-topology", ImmutableMap.of("schema", new SchemaNode(
        "wrong schema",
        Optional.of(keyFormat),
        Optional.of(valueFormat)
    )));

    givenActualTopology("the-topology", ImmutableMap.of("schema", new SchemaInfo(
        LOGICAL_SCHEMA,
        Optional.of(keyFormat),
        Optional.of(valueFormat)
    )));

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> executor.buildAndExecuteQuery(testCase, listener)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Schemas used by topology differ from those used by previous versions of KSQL "
            + "- this is likely to mean there is a non-backwards compatible change.\n"
            + "THIS IS BAD!\n"));
  }

  @Test
  public void shouldFailOnMismatchKeyFormat() {
    // Given:
    givenExpectedTopology("the-topology", ImmutableMap.of("schema", new SchemaNode(
        LOGICAL_SCHEMA.toString(),
        Optional.empty(),
        Optional.of(valueFormat)
    )));

    givenActualTopology("the-topology", ImmutableMap.of("schema", new SchemaInfo(
        LOGICAL_SCHEMA,
        Optional.of(keyFormat),
        Optional.of(valueFormat)
    )));

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> executor.buildAndExecuteQuery(testCase, listener)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Schemas used by topology differ from those used by previous versions of KSQL "
            + "- this is likely to mean there is a non-backwards compatible change.\n"
            + "THIS IS BAD!\n"));
  }

  @Test
  public void shouldFailOnMismatchValueFormat() {
    // Given:
    givenExpectedTopology("the-topology", ImmutableMap.of("schema", new SchemaNode(
        LOGICAL_SCHEMA.toString(),
        Optional.of(keyFormat),
        Optional.empty()
    )));

    givenActualTopology("the-topology", ImmutableMap.of("schema", new SchemaInfo(
        LOGICAL_SCHEMA,
        Optional.of(keyFormat),
        Optional.of(valueFormat)
    )));

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> executor.buildAndExecuteQuery(testCase, listener)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Schemas used by topology differ from those used by previous versions of KSQL "
            + "- this is likely to mean there is a non-backwards compatible change.\n"
            + "THIS IS BAD!\n"));
  }

  @Test
  public void shouldFailOnTooLittleOutput() {
    // Given:
    final ProducerRecord<byte[], byte[]> rec0 = producerRecord(sinkTopic, 123456719L, "k1", "v1");
    when(kafkaService.readRecords(SINK_TOPIC_NAME)).thenReturn(ImmutableList.of(rec0));

    final Record expected_0 = new Record(SINK_TOPIC_NAME, "k1", null, "v1", null, Optional.of(1L), null, Optional.empty());
    final Record expected_1 = new Record(SINK_TOPIC_NAME, "k1", null, "v1", null, Optional.of(1L), null, Optional.empty());
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.buildAndExecuteQuery(testCase, listener)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Expected <2> records but it was <1>\n"
            + "Actual records: \n"
            + "<k1, \"v1\"> with timestamp=123456719"));
  }

  @Test
  public void shouldFailOnTooMuchOutput() {
    // Given:
    final ProducerRecord<byte[], byte[]> rec0 = producerRecord(sinkTopic, 123456719L, "k1", "v1");
    final ProducerRecord<byte[], byte[]> rec1 = producerRecord(sinkTopic, 123456789L, "k2", "v2");
    when(kafkaService.readRecords(SINK_TOPIC_NAME)).thenReturn(ImmutableList.of(rec0, rec1));

    final Record expected_0 = new Record(SINK_TOPIC_NAME, "k1", null, "v1", null, Optional.of(1L), null, Optional.empty());
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> executor.buildAndExecuteQuery(testCase, listener)
    );

    // Then:
    assertThat(e.getMessage(), is("Topic sink_topic. Expected <1> records but it was <2>\n"
        + "Actual records: \n"
        + "<k1, \"v1\"> with timestamp=123456719 and headers=[]\n"
        + "<k2, \"v2\"> with timestamp=123456789 and headers=[]")
    );
  }

  @Test
  public void shouldFailOnUnexpectedOutput() {
    // Given:
    final ProducerRecord<byte[], byte[]> rec0 = producerRecord(sinkTopic, 123456719L, "k1", "v1");
    final ProducerRecord<byte[], byte[]> rec1 = producerRecord(sinkTopic, 123456789L, "k2", "v2");
    when(kafkaService.readRecords(SINK_TOPIC_NAME)).thenReturn(ImmutableList.of(rec0, rec1));

    final Record expected_0 = new Record(SINK_TOPIC_NAME, "k1", TextNode.valueOf("k1"), "v1", TextNode.valueOf("v1"),
        Optional.of(123456719L), null, Optional.empty());
    final Record expected_1 = new Record(SINK_TOPIC_NAME, "k2", TextNode.valueOf("k2"), "different",
        TextNode.valueOf("different"), Optional.of(123456789L), null, Optional.empty());
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> executor.buildAndExecuteQuery(testCase, listener)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Expected <\"k2\", \"different\"> with timestamp=123456789 and headers=[] but was <k2, \"v2\"> with timestamp=123456789 and headers=[]"));
  }

  @Test
  public void shouldFailOnMismatchedHeaders() {
    // Given:
    final TestHeader h1 = new TestHeader("a", new byte[] {12, 23});
    final TestHeader h2 = new TestHeader("b", new byte[] {123});
    final ProducerRecord<byte[], byte[]> rec0 = producerRecord(sinkTopic, 123456719L, "k1", "v1", Serdes.String().serializer(), ImmutableList.of(h1));
    final ProducerRecord<byte[], byte[]> rec1 = producerRecord(sinkTopic, 123456789L, "k2", "v2", Serdes.String().serializer(), ImmutableList.of(h1));
    when(kafkaService.readRecords(SINK_TOPIC_NAME)).thenReturn(ImmutableList.of(rec0, rec1));

    final Record expected_0 = new Record(SINK_TOPIC_NAME, "k1", TextNode.valueOf("k1"), "v1", TextNode.valueOf("v1"),
        Optional.of(123456719L), null, Optional.of(ImmutableList.of(h1)));
    final Record expected_1 = new Record(SINK_TOPIC_NAME, "k2", TextNode.valueOf("k2"), "v2", TextNode.valueOf("v2"),
        Optional.of(123456789L), null, Optional.of(ImmutableList.of(h2)));
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> executor.buildAndExecuteQuery(testCase, listener)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Expected <\"k2\", \"v2\"> with timestamp=123456789 and headers=[{KEY=b,VALUE=ew==}] but was <k2, \"v2\"> with timestamp=123456789 and headers=[{KEY=a,VALUE=DBc=}]"));
  }

  @Test
  public void shouldPassOnExpectedOutput() {
    // Given:
    final ProducerRecord<byte[], byte[]> rec0 = producerRecord(sinkTopic, 123456719L, "k1", "v1");
    final ProducerRecord<byte[], byte[]> rec1 = producerRecord(sinkTopic, 123456789L, "k2", "v2");
    when(kafkaService.readRecords(SINK_TOPIC_NAME)).thenReturn(ImmutableList.of(rec0, rec1));

    final Record expected_0 = new Record(SINK_TOPIC_NAME, "k1", TextNode.valueOf("k1"), "v1", TextNode.valueOf("v1"),
        Optional.of(123456719L), null, Optional.empty());
    final Record expected_1 = new Record(SINK_TOPIC_NAME, "k2", TextNode.valueOf("k2"), "v2", TextNode.valueOf("v2"),
        Optional.of(123456789L), null, Optional.empty());
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    // When:
    executor.buildAndExecuteQuery(testCase, listener);

    // Then: no exception.
  }

  @Test
  public void shouldHandleNonStringKeys() {
    // Given:
    final ProducerRecord<byte[], byte[]> rec0 = producerRecord(sinkTopic, 123456719L, 1, "v1");
    final ProducerRecord<byte[], byte[]> rec1 = producerRecord(sinkTopic, 123456789L, 1, "v2");
    when(kafkaService.readRecords(SINK_TOPIC_NAME)).thenReturn(ImmutableList.of(rec0, rec1));

    final Record expected_0 = new Record(SINK_TOPIC_NAME, 1, IntNode.valueOf(1), "v1", TextNode.valueOf("v1"),
        Optional.of(123456719L), null, Optional.empty());
    final Record expected_1 = new Record(SINK_TOPIC_NAME, 1, IntNode.valueOf(1), "v2", TextNode.valueOf("v2"),
        Optional.of(123456789L), null, Optional.empty());
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("key"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
        .build();

    givenDataSourceTopic(schema);

    // When:
    executor.buildAndExecuteQuery(testCase, listener);

    // Then: no exception.
  }

  @Test
  public void shouldCheckPostConditions() {
    // When:
    executor.buildAndExecuteQuery(testCase, listener);

    // Then:
    verify(postConditions).verify(
        metaStore,
        ImmutableList.of(
            new PostTopicNode(
                sinkTopic.getName(),
                KeyFormat.nonWindowed(FormatInfo.of("Kafka"), SerdeFeatures.of()),
                ValueFormat.of(FormatInfo.of("Json"), SerdeFeatures.of()),
                OptionalInt.empty(),
                NullNode.getInstance(),
                NullNode.getInstance()
            )
        )
    );
  }

  private void givenExpectedTopology(final String topology) {
    when(testCase.getExpectedTopology()).thenReturn(Optional.of(expectedTopologyAndConfig));
    when(expectedTopologyAndConfig.getTopology()).thenReturn(topology);
  }

  private void givenExpectedTopology(final String topology, final Map<String, SchemaNode> schemas) {
    givenExpectedTopology(topology);
    when(expectedTopologyAndConfig.getSchemas()).thenReturn(schemas);
  }

  private void givenActualTopology(final String topology) {
    when(testCase.getGeneratedTopologies()).thenReturn(ImmutableList.of(topology));
  }

  private void givenActualTopology(
      final String topology,
      final Map<String, QuerySchemas.SchemaInfo> schemas
  ) {
    givenActualTopology(topology);
    when(testCase.getGeneratedSchemas()).thenReturn(schemas);
  }

  private void givenDataSourceTopic(final LogicalSchema schema) {
    final KsqlTopic topic = mock(KsqlTopic.class);
    when(topic.getKeyFormat())
        .thenReturn(KeyFormat.of(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of(),
            Optional.empty()
        ));
    when(topic.getValueFormat())
        .thenReturn(ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of()));

    final SourceName sourceName = SourceName.of(TestExecutorTest.SINK_TOPIC_NAME + "_source");

    final DataSource dataSource = mock(DataSource.class);
    when(dataSource.getKsqlTopic()).thenReturn(topic);
    when(dataSource.getSchema()).thenReturn(schema);
    when(dataSource.getKafkaTopicName()).thenReturn(TestExecutorTest.SINK_TOPIC_NAME);
    when(dataSource.getName()).thenReturn(sourceName);
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    allSources.put(sourceName, dataSource);
  }

  private static ProducerRecord<byte[], byte[]> producerRecord(
      final Topic topic,
      final long rowTime,
      final String key,
      final String value
  ) {
    return producerRecord(topic, rowTime, key, value, Serdes.String().serializer(), ImmutableList.of());
  }

  private static ProducerRecord<byte[], byte[]> producerRecord(
      final Topic topic,
      final long rowTime,
      final int key,
      final String value
  ) {
    return producerRecord(topic, rowTime, key, value, Serdes.Integer().serializer(), ImmutableList.of());
  }

  private static <K> ProducerRecord<byte[], byte[]> producerRecord(
      final Topic topic,
      final long rowTime,
      final K key,
      final String value,
      final Serializer<K> keySerializer,
      final List<Header> headers
  ) {
    final byte[] serializedKey = keySerializer.serialize("", key);
    final byte[] serializeValue = new ValueSpecJsonSerdeSupplier(false, ImmutableMap.of())
        .getSerializer(null, false)
        .serialize("", value);

    return new ProducerRecord<>(
        topic.getName(),
        1,
        rowTime,
        serializedKey,
        serializeValue,
        headers
    );
  }
}