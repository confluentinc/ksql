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

import static org.hamcrest.Matchers.containsString;
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
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.tools.TestExecutor.TopologyBuilder;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.test.tools.stubs.StubKafkaRecord;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
@SuppressWarnings("ResultOfMethodCallIgnored")
@RunWith(MockitoJUnitRunner.class)
public class TestExecutorTest {

  private static final String INTERNAL_TOPIC_0 = "internal";
  private static final String SINK_TOPIC_NAME = "sink_topic";

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
      .build();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
  private Function<TopologyTestDriver, Set<String>> internalTopicsAccessor;

  private TestExecutor executor;
  private final Map<SourceName, DataSource<?>> allSources = new HashMap<>();

  @Before
  public void setUp() {
    allSources.clear();

    executor = new TestExecutor(
        kafkaService,
        serviceContext,
        ksqlEngine,
        topologyBuilder,
        internalTopicsAccessor
    );

    when(sourceTopic.getName()).thenReturn("source_topic");
    when(sinkTopic.getName()).thenReturn(SINK_TOPIC_NAME);

    final TopologyTestDriverContainer container = TopologyTestDriverContainer.of(
        topologyTestDriver,
        ImmutableList.of(sourceTopic),
        sinkTopic
    );

    when(topologyBuilder.buildStreamsTopologyTestDrivers(any(), any(), any(), any(), any()))
        .thenReturn(ImmutableList.of(container));

    when(testCase.getPostConditions()).thenReturn(postConditions);

    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);

    when(metaStore.getAllDataSources()).thenReturn(allSources);

    when(internalTopicsAccessor.apply(topologyTestDriver))
        .thenReturn(ImmutableSet.of(INTERNAL_TOPIC_0));

    givenDataSourceTopic(SCHEMA);
  }

  @Test
  public void shouldNotVerifyTopologyIfNotSet() {
    // Given:
    when(testCase.getExpectedTopology()).thenReturn(Optional.empty());

    // When:
    executor.buildAndExecuteQuery(testCase);

    // Then: did not throw, and
    verify(testCase, never()).getGeneratedTopologies();
  }

  @Test
  public void shouldVerifyTopology() {
    // Given:
    givenExpectedTopology("matching-topology");
    givenActualTopology("matching-topology");

    // When:
    executor.buildAndExecuteQuery(testCase);

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
    executor.buildAndExecuteQuery(testCase);

    // Then: did not throw, and
    verify(testCase).getGeneratedSchemas();
  }

  @Test
  public void shouldFailOnTopologyMismatch() {
    // Given:
    givenExpectedTopology("expected-topology");
    givenActualTopology("actual-topology");

    // Then:
    expectedException.expect(AssertionError.class);
    expectedException.expectMessage(
        "Generated topology differs from that built by previous versions of KSQL "
            + "- this likely means there is a non-backwards compatible change.\n"
            + "THIS IS BAD!\n"
            + "Expected: is \"expected-topology\"\n"
            + "     but: was \"actual-topology\"");

    // When:
    executor.buildAndExecuteQuery(testCase);
  }

  @Test
  public void shouldFailOnSchemasMismatch() {
    // Given:
    givenExpectedTopology("the-topology", ImmutableMap.of("expected", "schemas"));
    givenActualTopology("the-topology", ImmutableMap.of("actual", "schemas"));

    // Then:
    expectedException.expect(AssertionError.class);
    expectedException.expectMessage(containsString(
        "Schemas used by topology differ from those used by previous versions of KSQL "
            + "- this is likely to mean there is a non-backwards compatible change.\n"
            + "THIS IS BAD!\n"));

    // When:
    executor.buildAndExecuteQuery(testCase);
  }

  @Test
  public void shouldFailOnTwoLittleOutput() {
    // Given:
    final StubKafkaRecord actual_0 = kafkaRecord(sinkTopic, 123456719L, "k1", "v1");
    when(kafkaService.readRecords(SINK_TOPIC_NAME)).thenReturn(ImmutableList.of(actual_0));

    final Record expected_0 = new Record(sinkTopic, "k1", "v1", null, Optional.of(1L), null);
    final Record expected_1 = new Record(sinkTopic, "k1", "v1", null, Optional.of(1L), null);
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    // Expect
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Expected <2> records but it was <1>\n"
        + "Actual records: \n"
        + "<k1, \"v1\"> with timestamp=123456719");

    // When:
    executor.buildAndExecuteQuery(testCase);
  }

  @Test
  public void shouldFailOnTwoMuchOutput() {
    // Given:
    final StubKafkaRecord actual_0 = kafkaRecord(sinkTopic, 123456719L, "k1", "v1");
    final StubKafkaRecord actual_1 = kafkaRecord(sinkTopic, 123456789L, "k2", "v2");
    when(kafkaService.readRecords(SINK_TOPIC_NAME))
        .thenReturn(ImmutableList.of(actual_0, actual_1));

    final Record expected_0 = new Record(sinkTopic, "k1", "v1", null, Optional.of(1L), null);
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0));

    // Expect
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Expected <1> records but it was <2>\n"
        + "Actual records: \n"
        + "<k1, \"v1\"> with timestamp=123456719 \n"
        + "<k2, \"v2\"> with timestamp=123456789");

    // When:
    executor.buildAndExecuteQuery(testCase);
  }

  @Test
  public void shouldFailOnUnexpectedOutput() {
    // Given:
    final StubKafkaRecord actual_0 = kafkaRecord(sinkTopic, 123456719L, "k1", "v1");
    final StubKafkaRecord actual_1 = kafkaRecord(sinkTopic, 123456789L, "k2", "v2");
    when(kafkaService.readRecords(SINK_TOPIC_NAME))
        .thenReturn(ImmutableList.of(actual_0, actual_1));

    final Record expected_0 = new Record(sinkTopic, "k1", "v1", TextNode.valueOf("v1"), Optional.of(123456719L), null);
    final Record expected_1 = new Record(sinkTopic, "k2", "different", TextNode.valueOf("different"), Optional.of(123456789L), null);
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    // Expect
    expectedException.expect(AssertionError.class);
    expectedException.expectMessage(
        "Expected <k2, \"different\"> with timestamp=123456789 but was <k2, \"v2\"> with timestamp=123456789");

    // When:
    executor.buildAndExecuteQuery(testCase);
  }

  @Test
  public void shouldPassOnExpectedOutput() {
    // Given:
    final StubKafkaRecord actual_0 = kafkaRecord(sinkTopic, 123456719L, "k1", "v1");
    final StubKafkaRecord actual_1 = kafkaRecord(sinkTopic, 123456789L, "k2", "v2");
    when(kafkaService.readRecords(SINK_TOPIC_NAME))
        .thenReturn(ImmutableList.of(actual_0, actual_1));

    final Record expected_0 = new Record(sinkTopic, "k1", "v1", TextNode.valueOf("v1"), Optional.of(123456719L), null);
    final Record expected_1 = new Record(sinkTopic, "k2", "v2", TextNode.valueOf("v2"), Optional.of(123456789L), null);
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    // When:
    executor.buildAndExecuteQuery(testCase);

    // Then: no exception.
  }

  @Test
  public void shouldHandleNonStringKeys() {
    // Given:
    final StubKafkaRecord actual_0 = kafkaRecord(sinkTopic, 123456719L, 1, "v1");
    final StubKafkaRecord actual_1 = kafkaRecord(sinkTopic, 123456789L, 1, "v2");
    when(kafkaService.readRecords(SINK_TOPIC_NAME))
        .thenReturn(ImmutableList.of(actual_0, actual_1));

    final Record expected_0 = new Record(sinkTopic, 1, "v1", TextNode.valueOf("v1"), Optional.of(123456719L), null);
    final Record expected_1 = new Record(sinkTopic, 1, "v2", TextNode.valueOf("v2"), Optional.of(123456789L), null);
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("key"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
        .build();

    givenDataSourceTopic(schema);

    // When:
    executor.buildAndExecuteQuery(testCase);

    // Then: no exception.
  }

  @Test
  public void shouldCheckPostConditions() {
    // When:
    executor.buildAndExecuteQuery(testCase);

    // Then:
    verify(postConditions).verify(metaStore, ImmutableSet.of(INTERNAL_TOPIC_0));
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
        .thenReturn(KeyFormat.of(FormatInfo.of(Format.KAFKA), Optional.empty()));
    final DataSource<?> dataSource = mock(DataSource.class);
    when(dataSource.getKsqlTopic()).thenReturn(topic);
    when(dataSource.getSchema()).thenReturn(schema);
    when(dataSource.getKafkaTopicName()).thenReturn(TestExecutorTest.SINK_TOPIC_NAME);
    allSources.put(SourceName.of(TestExecutorTest.SINK_TOPIC_NAME + "_source"), dataSource);
  }

  private static StubKafkaRecord kafkaRecord(
      final Topic topic,
      final long rowTime,
      final Object key,
      final String value
  ) {
    final ProducerRecord<Object, String> record = new ProducerRecord<>(
        topic.getName(),
        1,
        rowTime,
        key,
        value
    );

    return StubKafkaRecord.of(topic, record);
  }
}