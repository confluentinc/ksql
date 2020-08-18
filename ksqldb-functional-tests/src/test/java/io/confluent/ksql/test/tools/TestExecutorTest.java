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
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.model.PostConditionsNode.PostTopicNode;
import io.confluent.ksql.test.tools.TestExecutor.TopologyBuilder;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
@SuppressWarnings("ResultOfMethodCallIgnored")
@RunWith(MockitoJUnitRunner.class)
public class TestExecutorTest {

  private static final String SINK_TOPIC_NAME = "sink_topic";

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
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

  private TestExecutor executor;
  private final Map<SourceName, DataSource> allSources = new HashMap<>();

  @Before
  public void setUp() {
    allSources.clear();

    when(serviceContext.getSchemaRegistryClient()).thenReturn(srClient);

    executor = new TestExecutor(
        kafkaService,
        serviceContext,
        ksqlEngine,
        topologyBuilder
    );

    when(sourceTopic.getName()).thenReturn("source_topic");
    when(sinkTopic.getName()).thenReturn(SINK_TOPIC_NAME);

    final TopologyTestDriverContainer container = TopologyTestDriverContainer.of(
        topologyTestDriver,
        ImmutableList.of(sourceTopic),
        sinkTopic
    );

    when(topologyTestDriver.producedTopicNames()).thenReturn(ImmutableSet.of(SINK_TOPIC_NAME));

    when(topologyBuilder.buildStreamsTopologyTestDrivers(any(), any(), any(), any(), any(), any()))
        .thenReturn(ImmutableList.of(container));

    when(testCase.getPostConditions()).thenReturn(postConditions);

    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);

    when(metaStore.getAllDataSources()).thenReturn(allSources);

    givenDataSourceTopic(SCHEMA);
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
    givenExpectedTopology("a-topology", ImmutableMap.of("matching", "schemas"));
    givenActualTopology("a-topology", ImmutableMap.of("matching", "schemas"));

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
  public void shouldFailOnSchemasMismatch() {
    // Given:
    givenExpectedTopology("the-topology", ImmutableMap.of("expected", "schemas"));
    givenActualTopology("the-topology", ImmutableMap.of("actual", "schemas"));

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
    final ProducerRecord<?, ?> rec0 = producerRecord(sinkTopic, 123456719L, "k1", "v1");
    when(kafkaService.readRecords(SINK_TOPIC_NAME)).thenReturn(ImmutableList.of(rec0));

    final Record expected_0 = new Record(SINK_TOPIC_NAME, "k1", "v1", null, Optional.of(1L), null);
    final Record expected_1 = new Record(SINK_TOPIC_NAME, "k1", "v1", null, Optional.of(1L), null);
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
    final ProducerRecord<?, ?> rec0 = producerRecord(sinkTopic, 123456719L, "k1", "v1");
    final ProducerRecord<?, ?> rec1 = producerRecord(sinkTopic, 123456789L, "k2", "v2");
    when(kafkaService.readRecords(SINK_TOPIC_NAME)).thenReturn(ImmutableList.of(rec0, rec1));

    final Record expected_0 = new Record(SINK_TOPIC_NAME, "k1", "v1", null, Optional.of(1L), null);
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> executor.buildAndExecuteQuery(testCase, listener)
    );

    // Then:
    assertThat(e.getMessage(), is("Topic sink_topic. Expected <1> records but it was <2>\n"
        + "Actual records: \n"
        + "<k1, \"v1\"> with timestamp=123456719\n"
        + "<k2, \"v2\"> with timestamp=123456789")
    );
  }

  @Test
  public void shouldFailOnUnexpectedOutput() {
    // Given:
    final ProducerRecord<?, ?> rec0 = producerRecord(sinkTopic, 123456719L, "k1", "v1");
    final ProducerRecord<?, ?> rec1 = producerRecord(sinkTopic, 123456789L, "k2", "v2");
    when(kafkaService.readRecords(SINK_TOPIC_NAME)).thenReturn(ImmutableList.of(rec0, rec1));

    final Record expected_0 = new Record(SINK_TOPIC_NAME, "k1", "v1", TextNode.valueOf("v1"),
        Optional.of(123456719L), null);
    final Record expected_1 = new Record(SINK_TOPIC_NAME, "k2", "different",
        TextNode.valueOf("different"), Optional.of(123456789L), null);
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> executor.buildAndExecuteQuery(testCase, listener)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Expected <k2, \"different\"> with timestamp=123456789 but was <k2, \"v2\"> with timestamp=123456789"));
  }

  @Test
  public void shouldPassOnExpectedOutput() {
    // Given:
    final ProducerRecord<?, ?> rec0 = producerRecord(sinkTopic, 123456719L, "k1", "v1");
    final ProducerRecord<?, ?> rec1 = producerRecord(sinkTopic, 123456789L, "k2", "v2");
    when(kafkaService.readRecords(SINK_TOPIC_NAME)).thenReturn(ImmutableList.of(rec0, rec1));

    final Record expected_0 = new Record(SINK_TOPIC_NAME, "k1", "v1", TextNode.valueOf("v1"),
        Optional.of(123456719L), null);
    final Record expected_1 = new Record(SINK_TOPIC_NAME, "k2", "v2", TextNode.valueOf("v2"),
        Optional.of(123456789L), null);
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    // When:
    executor.buildAndExecuteQuery(testCase, listener);

    // Then: no exception.
  }

  @Test
  public void shouldHandleNonStringKeys() {
    // Given:
    final ProducerRecord<?, ?> rec0 = producerRecord(sinkTopic, 123456719L, 1, "v1");
    final ProducerRecord<?, ?> rec1 = producerRecord(sinkTopic, 123456789L, 1, "v2");
    when(kafkaService.readRecords(SINK_TOPIC_NAME)).thenReturn(ImmutableList.of(rec0, rec1));

    final Record expected_0 = new Record(SINK_TOPIC_NAME, 1, "v1", TextNode.valueOf("v1"),
        Optional.of(123456719L), null);
    final Record expected_1 = new Record(SINK_TOPIC_NAME, 1, "v2", TextNode.valueOf("v2"),
        Optional.of(123456789L), null);
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
                KeyFormat.nonWindowed(FormatInfo.of("Kafka")),
                ValueFormat.of(FormatInfo.of("Json")),
                OptionalInt.empty()
            )
        )
    );
  }

  private void givenExpectedTopology(final String topology) {
    when(testCase.getExpectedTopology()).thenReturn(Optional.of(expectedTopologyAndConfig));
    when(expectedTopologyAndConfig.getTopology()).thenReturn(topology);
  }

  private void givenExpectedTopology(final String topology, final Map<String, String> schemas) {
    givenExpectedTopology(topology);
    when(expectedTopologyAndConfig.getSchemas()).thenReturn(schemas);
  }

  private void givenActualTopology(final String topology) {
    when(testCase.getGeneratedTopologies()).thenReturn(ImmutableList.of(topology));
  }

  private void givenActualTopology(final String topology, final Map<String, String> schemas) {
    givenActualTopology(topology);
    when(testCase.getGeneratedSchemas()).thenReturn(schemas);
  }

  private void givenDataSourceTopic(final LogicalSchema schema) {
    final KsqlTopic topic = mock(KsqlTopic.class);
    when(topic.getKeyFormat())
        .thenReturn(KeyFormat.of(FormatInfo.of(FormatFactory.KAFKA.name()), Optional.empty()));
    when(topic.getValueFormat())
        .thenReturn(ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name())));

    final SourceName sourceName = SourceName.of(TestExecutorTest.SINK_TOPIC_NAME + "_source");

    final DataSource dataSource = mock(DataSource.class);
    when(dataSource.getKsqlTopic()).thenReturn(topic);
    when(dataSource.getSchema()).thenReturn(schema);
    when(dataSource.getKafkaTopicName()).thenReturn(TestExecutorTest.SINK_TOPIC_NAME);
    when(dataSource.getName()).thenReturn(sourceName);
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(dataSource.getSerdeOptions()).thenReturn(Collections.emptySet());
    allSources.put(sourceName, dataSource);
  }

  private static ProducerRecord<?, ?> producerRecord(
      final Topic topic,
      final long rowTime,
      final Object key,
      final String value
  ) {
    return new ProducerRecord<>(
        topic.getName(),
        1,
        rowTime,
        key,
        value
    );
  }
}