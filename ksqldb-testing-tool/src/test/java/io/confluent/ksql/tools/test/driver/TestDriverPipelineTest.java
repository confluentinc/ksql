/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.tools.test.driver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.tools.test.driver.TestDriverPipeline.TopicInfo;
import io.confluent.ksql.util.KsqlException;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class TestDriverPipelineTest {

  private static final GenericKey KEY = GenericKey.genericKey("key");

  private static final GenericRow ROW1 = new GenericRow(1);
  private static final GenericRow ROW2 = new GenericRow(2);

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private final ListMultimap<String, TestRecord<GenericKey, GenericRow>> outputs =
      ArrayListMultimap.create();

  @Mock
  private Serde<GenericKey> keySerde;
  @Mock
  private Serde<GenericRow> valueSerde;

  private TestDriverPipeline pipeline;

  @Before
  public void setUp() {
    outputs.clear();
    pipeline = new TestDriverPipeline();
  }

  @Test
  public void shouldHandleSingleTopology() {
    // Given (a topology that pipes a directly to b):
    final TopologyTestDriver driver = mock(TopologyTestDriver.class);
    final TestInputTopic<GenericKey, GenericRow> inA = givenInput(driver, "a");
    givenOutput(driver, "b");

    givenPipe(inA, "b");

    pipeline.addDriver(driver, ImmutableList.of(inf("a")), inf("b"));

    // When:
    pipeline.pipeInput("a", KEY, ROW1, 1);

    // Then:
    assertThat(
        pipeline.getAllRecordsForTopic("b"),
        is(ImmutableList.of(new TestRecord<>(KEY, ROW1, Instant.ofEpochMilli(1))))
    );
  }

  @Test
  public void shouldHandleLinearChainTopology() {
    // Given (a topology that pipes a directly to b, and from b to c):
    final TopologyTestDriver driver = mock(TopologyTestDriver.class);
    final TestInputTopic<GenericKey, GenericRow> inA = givenInput(driver, "a");
    givenOutput(driver, "b");

    final TestInputTopic<GenericKey, GenericRow> inB = givenInput(driver, "b");
    givenOutput(driver, "c");

    givenPipe(inA, "b");
    givenPipe(inB, "c");

    pipeline.addDriver(driver, ImmutableList.of(inf("a")), inf("b"));
    pipeline.addDriver(driver, ImmutableList.of(inf("b")), inf("c"));

    // When:
    pipeline.pipeInput("a", KEY, ROW1, 1);

    // Then:
    assertThat(
        pipeline.getAllRecordsForTopic("b"),
        is(ImmutableList.of(new TestRecord<>(KEY, ROW1, Instant.ofEpochMilli(1))))
    );
    assertThat(
        pipeline.getAllRecordsForTopic("c"),
        is(ImmutableList.of(new TestRecord<>(KEY, ROW1, Instant.ofEpochMilli(1))))
    );
  }

  @Test
  public void shouldHandleMultiSourceTopology() {
    // Given (a topology that pipes a and b directly to c):
    final TopologyTestDriver driver = mock(TopologyTestDriver.class);
    final TestInputTopic<GenericKey, GenericRow> inA = givenInput(driver, "a");
    final TestInputTopic<GenericKey, GenericRow> inB = givenInput(driver, "b");
    givenOutput(driver, "c");

    givenPipe(inA, "c");
    givenPipe(inB, "c");

    pipeline.addDriver(driver, ImmutableList.of(inf("a"), inf("b")), inf("c"));

    // When:
    pipeline.pipeInput("a", KEY, ROW1, 1);
    pipeline.pipeInput("b", KEY, ROW2, 1);

    // Then:
    assertThat(
        pipeline.getAllRecordsForTopic("c"),
        is(ImmutableList.of(
            new TestRecord<>(KEY, ROW1, Instant.ofEpochMilli(1)),
            new TestRecord<>(KEY, ROW2, Instant.ofEpochMilli(1))
        ))
    );
  }

  @Test
  public void shouldHandleTwoTopologiesWithSameInputDifferentOutput() {
    // Given (two topologies that pipe a to b and a to c):
    final TopologyTestDriver driver1 = mock(TopologyTestDriver.class);
    final TopologyTestDriver driver2 = mock(TopologyTestDriver.class);
    final TestInputTopic<GenericKey, GenericRow> inA1 = givenInput(driver1, "a");
    final TestInputTopic<GenericKey, GenericRow> inA2 = givenInput(driver2, "a");

    givenOutput(driver1, "b");
    givenOutput(driver2, "c");

    givenPipe(inA1, "b");
    givenPipe(inA2, "c");

    pipeline.addDriver(driver1, ImmutableList.of(inf("a")), inf("b"));
    pipeline.addDriver(driver2, ImmutableList.of(inf("a")), inf("c"));

    // When:
    pipeline.pipeInput("a", KEY, ROW1, 1);

    // Then:
    assertThat(
        pipeline.getAllRecordsForTopic("b"),
        is(ImmutableList.of(
            new TestRecord<>(KEY, ROW1, Instant.ofEpochMilli(1))
        ))
    );
    assertThat(
        pipeline.getAllRecordsForTopic("c"),
        is(ImmutableList.of(
            new TestRecord<>(KEY, ROW1, Instant.ofEpochMilli(1))
        ))
    );
  }

  @Test
  public void shouldHandleOneOutputIsInputToTwoTopologies() {
    // Given:
    final TopologyTestDriver driver1 = mock(TopologyTestDriver.class);
    final TopologyTestDriver driver2 = mock(TopologyTestDriver.class);
    final TopologyTestDriver driver3 = mock(TopologyTestDriver.class);

    final TestInputTopic<GenericKey, GenericRow> inA = givenInput(driver1, "a");
    givenOutput(driver1, "b");

    final TestInputTopic<GenericKey, GenericRow> inB2 = givenInput(driver2, "b");
    givenOutput(driver2, "c");

    final TestInputTopic<GenericKey, GenericRow> inB3 = givenInput(driver3, "b");
    givenOutput(driver3, "d");

    givenPipe(inA, "b");
    givenPipe(inB2, "c");
    givenPipe(inB3, "d");

    pipeline.addDriver(driver1, ImmutableList.of(inf("a")), inf("b"));
    pipeline.addDriver(driver2, ImmutableList.of(inf("b")), inf("c"));
    pipeline.addDriver(driver3, ImmutableList.of(inf("b")), inf("d"));

    // When:
    pipeline.pipeInput("a", KEY, ROW1, 1);

    // Then:
    assertThat(
        pipeline.getAllRecordsForTopic("b"),
        is(ImmutableList.of(
            new TestRecord<>(KEY, ROW1, Instant.ofEpochMilli(1))
        ))
    );
    assertThat(
        pipeline.getAllRecordsForTopic("c"),
        is(ImmutableList.of(
            new TestRecord<>(KEY, ROW1, Instant.ofEpochMilli(1))
        ))
    );
    assertThat(
        pipeline.getAllRecordsForTopic("d"),
        is(ImmutableList.of(
            new TestRecord<>(KEY, ROW1, Instant.ofEpochMilli(1))
        ))
    );
  }

  @Test
  public void shouldDetectLoops() {
    // Given:
    final TopologyTestDriver driver1 = mock(TopologyTestDriver.class);
    final TopologyTestDriver driver2 = mock(TopologyTestDriver.class);

    final TestInputTopic<GenericKey, GenericRow> inA = givenInput(driver1, "a");
    givenOutput(driver1, "b");
    final TestInputTopic<GenericKey, GenericRow> inB = givenInput(driver2, "b");
    givenOutput(driver2, "a");

    givenPipe(inA, "b");
    givenPipe(inB, "a");

    pipeline.addDriver(driver1, ImmutableList.of(inf("a")), inf("b"));
    pipeline.addDriver(driver2, ImmutableList.of(inf("b")), inf("a"));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> pipeline.pipeInput("a", KEY, ROW1, 1));

    // Then:
    assertThat(e.getMessage(), containsString("Detected illegal cycle in topology: a->b->a"));
  }

  private TopicInfo inf(final String name) {
    return new TopicInfo(name, keySerde, valueSerde);
  }

  private TestRecord<GenericKey, GenericRow> fromInv(final InvocationOnMock inv) {
    return new TestRecord<>(
        inv.getArgument(0),
        inv.getArgument(1),
        Instant.ofEpochMilli(inv.getArgument(2)));
  }

  private void givenPipe(final TestInputTopic<GenericKey, GenericRow> from, final String to) {
    doAnswer(inv -> outputs.put(to, fromInv(inv)))
        .when(from)
        .pipeInput(any(GenericKey.class), any(GenericRow.class), any(long.class));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private TestInputTopic<GenericKey, GenericRow> givenInput(
      final TopologyTestDriver driver,
      final String topic
  ) {
    TestInputTopic input = mock(TestInputTopic.class);
    when(driver.createInputTopic(eq(topic), any(), any())).thenReturn(input);
    return input;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void givenOutput(
      final TopologyTestDriver driver,
      final String topic
  ) {
    TestOutputTopic output = mock(TestOutputTopic.class);
    when(driver.createOutputTopic(eq(topic), any(), any())).thenReturn(output);
    when(output.readRecordsToList()).thenAnswer(inv -> outputs.removeAll(topic));
  }

}