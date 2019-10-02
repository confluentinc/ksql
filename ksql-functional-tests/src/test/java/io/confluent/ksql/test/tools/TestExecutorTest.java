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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.tools.TestExecutor.TopologyBuilder;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.test.tools.stubs.StubKafkaRecord;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.util.KsqlException;
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

  @Before
  public void setUp() {
    executor = new TestExecutor(
        kafkaService,
        serviceContext,
        ksqlEngine,
        topologyBuilder,
        internalTopicsAccessor
    );

    when(sourceTopic.getName()).thenReturn("source_topic");
    when(sinkTopic.getName()).thenReturn("sink_topic");

    final TopologyTestDriverContainer container = TopologyTestDriverContainer.of(
        topologyTestDriver,
        ImmutableList.of(sourceTopic),
        sinkTopic
    );

    when(topologyBuilder.buildStreamsTopologyTestDrivers(any(), any(), any(), any(), any()))
        .thenReturn(ImmutableList.of(container));

    when(testCase.getPostConditions()).thenReturn(postConditions);

    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);

    when(internalTopicsAccessor.apply(topologyTestDriver))
        .thenReturn(ImmutableSet.of(INTERNAL_TOPIC_0));
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
    givenExpectedTopology("a-topology", "matching-schemas");
    givenActualTopology("a-topology", "matching-schemas");

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
    givenExpectedTopology("the-topology", "expected-schemas");
    givenActualTopology("the-topology", "actual-schemas");

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
    when(kafkaService.readRecords("sink_topic")).thenReturn(ImmutableList.of(actual_0));

    final Record expected_0 = new Record(sinkTopic, "k1", "v1", 1L, null);
    final Record expected_1 = new Record(sinkTopic, "k1", "v1", 1L, null);
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    // Expect
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Expected <2> records but it was <1>\n"
        + "Actual records: \n"
        + "<k1, v1> with timestamp=123456719");

    // When:
    executor.buildAndExecuteQuery(testCase);
  }

  @Test
  public void shouldFailOnTwoMuchOutput() {
    // Given:
    final StubKafkaRecord actual_0 = kafkaRecord(sinkTopic, 123456719L, "k1", "v1");
    final StubKafkaRecord actual_1 = kafkaRecord(sinkTopic, 123456789L, "k2", "v2");
    when(kafkaService.readRecords("sink_topic")).thenReturn(ImmutableList.of(actual_0, actual_1));

    final Record expected_0 = new Record(sinkTopic, "k1", "v1", 1L, null);
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0));

    // Expect
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Expected <1> records but it was <2>\n"
        + "Actual records: \n"
        + "<k1, v1> with timestamp=123456719 \n"
        + "<k2, v2> with timestamp=123456789");

    // When:
    executor.buildAndExecuteQuery(testCase);
  }

  @Test
  public void shouldFailOnUnexpectedOutput() {
    // Given:
    final StubKafkaRecord actual_0 = kafkaRecord(sinkTopic, 123456719L, "k1", "v1");
    final StubKafkaRecord actual_1 = kafkaRecord(sinkTopic, 123456789L, "k2", "v2");
    when(kafkaService.readRecords("sink_topic")).thenReturn(ImmutableList.of(actual_0, actual_1));

    final Record expected_0 = new Record(sinkTopic, "k1", "v1", 123456719L, null);
    final Record expected_1 = new Record(sinkTopic, "k2", "different", 123456789L, null);
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

    // Expect
    expectedException.expect(AssertionError.class);
    expectedException.expectMessage(
        "Expected <k2, different> with timestamp=123456789 but was <k2, v2> with timestamp=123456789");

    // When:
    executor.buildAndExecuteQuery(testCase);
  }

  @Test
  public void shouldPassOnExpectedOutput() {
    // Given:
    final StubKafkaRecord actual_0 = kafkaRecord(sinkTopic, 123456719L, "k1", "v1");
    final StubKafkaRecord actual_1 = kafkaRecord(sinkTopic, 123456789L, "k2", "v2");
    when(kafkaService.readRecords("sink_topic")).thenReturn(ImmutableList.of(actual_0, actual_1));

    final Record expected_0 = new Record(sinkTopic, "k1", "v1", 123456719L, null);
    final Record expected_1 = new Record(sinkTopic, "k2", "v2", 123456789L, null);
    when(testCase.getOutputRecords()).thenReturn(ImmutableList.of(expected_0, expected_1));

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

  private void givenExpectedTopology(final String topology, final String schemas) {
    givenExpectedTopology(topology);
    when(expectedTopologyAndConfig.getSchemas()).thenReturn(Optional.of(schemas));
  }

  private void givenActualTopology(final String topology) {
    when(testCase.getGeneratedTopologies()).thenReturn(ImmutableList.of(topology));
  }

  private void givenActualTopology(final String topology, final String schemas) {
    givenActualTopology(topology);
    when(testCase.getGeneratedSchemas()).thenReturn(ImmutableList.of(schemas));
  }

  private static StubKafkaRecord kafkaRecord(
      final Topic topic,
      final long rowTime,
      final String key,
      final String value
  ) {
    final ProducerRecord<String, String> record = new ProducerRecord<>(
        topic.getName(),
        1,
        rowTime,
        key,
        value
    );

    return StubKafkaRecord.of(topic, record);
  }
}