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

package io.confluent.ksql.test.driver;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;

public class TestDriverPipeline {
  /*
   * The pipeline is represented as a DAG of input/output nodes. The key aspect
   * to notice is that each output topic may be the input topic for one or more
   * topologies. Since each TopologyTestDriver is independent from one another,
   * we need to pipe the output of one topology into each other topology that
   * uses that topic as an input node manually.
   *
   * Take, for example, the pipeline below.
   *
   *                                                  +----------+     +-------+
   *                                      +---------->+  bar.in  +---->+ topo2 |
   *                                      |           +----------+     +-------+
   *  +----------+     +-------+     +----+-----+
   *  |  foo.in  +---->+ topo1 +---->+  bar.out |
   *  +----------+     +-------+     +----+-----+
   *                                      |           +----------+     +-------+
   *                                      +---------->+  bar.in  +---->+ topo3 |
   *                                                  +----------+     +-------+
   *  +----------+     +-------+     +----------+
   *  |  foo.in  +---->+ topo4 +---->+  baz.out |
   *  +----------+     +-------+     +----------+
   *
   * There are four topologies which are all somehow related. topo1 and topo4 both
   * read from foo.in but produce to different output topics. The output topic for
   * topo1 (bar) is used as the input topic for two other topologies (topo2 as well
   * as topo3).
   *
   * To model this pipeline, we track a mapping from topic to its corresponding input
   * and output topics as well as a tree representation of the pipeline. In the diagram
   * above, each ".in" is an independent input node and each ".out" is an independent
   * output node (even if they share the same name).
   */

  // ----------------------------------------------------------------------------------
  private static final class Input {

    private final TestInputTopic<Struct, GenericRow> topic;
    private final List<Output> receivers;

    private Input(final TestInputTopic<Struct, GenericRow> topic) {
      this.topic = topic;
      this.receivers = new ArrayList<>();
    }
  }

  private static final class Output {

    private final String name;
    private final TestOutputTopic<Struct, GenericRow> topic;

    private Output(final String name, final TestOutputTopic<Struct, GenericRow> topic) {
      this.name = name;
      this.topic = topic;
    }
  }

  public static final class TopicInfo {
    final String name;
    final Serde<Struct> keySerde;
    final Serde<GenericRow> valueSerde;

    public TopicInfo(
        final String name,
        final Serde<Struct> keySerde,
        final Serde<GenericRow> valueSerde
    ) {
      this.name = name;
      this.keySerde = keySerde;
      this.valueSerde = valueSerde;
    }
  }

  // ----------------------------------------------------------------------------------

  private final ListMultimap<String, Input> inputs;
  private final ListMultimap<String, Output> outputs;
  private final ListMultimap<String, TestRecord<Struct, GenericRow>> outputCache;

  // this map indexes into the outputCache to track which records we've already
  // read - we don't need to worry about concurrent modification while iterating
  // because appends only happen at the end of the outputCache
  private final Map<String, Integer> assertPositions;

  public TestDriverPipeline() {
    inputs = ArrayListMultimap.create();
    outputs = ArrayListMultimap.create();
    outputCache = ArrayListMultimap.create();

    assertPositions = new HashMap<>();
  }

  public void addDriver(
      final TopologyTestDriver driver,
      final List<TopicInfo> inputTopics,
      final TopicInfo outputTopic
  ) {
    // first create the output node, since whenever an event is piped to the
    // input nodes we will need to read and propagate events to the output
    // node
    final Output output = new Output(
        outputTopic.name,
        driver.createOutputTopic(
            outputTopic.name,
            outputTopic.keySerde.deserializer(),
            outputTopic.valueSerde.deserializer())
    );
    outputs.put(outputTopic.name, output);

    for (TopicInfo inputTopic : inputTopics) {
      final Input input = new Input(
          driver.createInputTopic(
              inputTopic.name,
              inputTopic.keySerde.serializer(),
              inputTopic.valueSerde.serializer())
      );
      inputs.put(inputTopic.name, input);

      // whenever we pipe data into input, we will need to also propagate the
      // output for any topic that uses "outputTopic" as an input topic
      input.receivers.add(output);
    }
  }

  public void pipeInput(
      final String topic,
      final Struct key,
      final GenericRow value,
      final long timestampMs
  ) {
    pipeInput(topic, key, value, timestampMs, new HashSet<>(), topic);
  }

  private void pipeInput(
      final String topic,
      final Struct key,
      final GenericRow value,
      final long timestampMs,
      final Set<String> loopDetection,
      final String path
  ) {
    final boolean added = loopDetection.add(topic);
    if (!added) {
      throw new KsqlException("Detected illegal cycle in topology: " + path);
    }

    final List<Input> inputs = this.inputs.get(topic);
    if (inputs.isEmpty()) {
      throw new KsqlException("Cannot pipe input to unknown source: " + topic);
    }

    for (final Input input : inputs) {
      input.topic.pipeInput(key, value, timestampMs);

      // handle the fallout of piping in a record (propagation)
      for (final Output receiver : input.receivers) {
        for (final TestRecord<Struct, GenericRow> record : receiver.topic.readRecordsToList()) {
          outputCache.put(receiver.name, record);

          if (this.inputs.containsKey(receiver.name)) {
            pipeInput(
                receiver.name,
                record.key(),
                record.value(),
                record.timestamp(),
                new HashSet<>(loopDetection),
                path + "->" + receiver.name
            );
          }
        }
      }
    }
  }

  public List<TestRecord<Struct, GenericRow>> getAllRecordsForTopic(final String topic) {
    return outputCache.get(topic);
  }

  public Iterator<TestRecord<Struct, GenericRow>> getRecordsForTopic(final String topic) {
    return new Iterator<TestRecord<Struct, GenericRow>>() {
      @Override
      public boolean hasNext() {
        final int idx = assertPositions.getOrDefault(topic, 0);
        return outputCache.get(topic).size() > idx;
      }

      @Override
      public TestRecord<Struct, GenericRow> next() {
        final int idx = assertPositions.getOrDefault(topic, 0);
        assertPositions.put(topic, idx + 1);
        return outputCache.get(topic).get(idx);
      }
    };
  }

}
