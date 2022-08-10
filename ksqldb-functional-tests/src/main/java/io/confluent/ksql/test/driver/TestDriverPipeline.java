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
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;

public class TestDriverPipeline {
  /*
   * The pipeline is represented as a tree of topic nodes. Each node keeps track of what nodes
   * read from them, as well as the "pipes" flowing in and out of them.
   *
   * For example, the pipeline for
   *
   * CREATE STREAM B AS SELECT * FROM A;
   * CREATE STREAM C AS SELECT * FROM A;
   * CREATE STREAM D AS SELECT * FROM C;
   *
   * looks like:
   *                               ---------------
   *                               | Topic B     |
   *                           |---| Input pipes |
   *                           |   | Output pipes|
   *    ---------------        |   ---------------
   *    | Topic A     |        |
   *    | Input pipes |        |   ---------------
   *    | Output pipes|--------|   | Topic C     |         ----------------
   *    ----------- ---        |---| Input pipes |         | Topic D      |
   *                               | Output pipes|---------| Input pipes  |
   *                               ---------------         | Output pipes |
   *                                                       ----------------
   */

  // ----------------------------------------------------------------------------------

  public static final class TopicInfo {
    final String name;
    final Serde<GenericKey> keySerde;
    final Serde<GenericRow> valueSerde;

    public TopicInfo(
        final String name,
        final Serde<GenericKey> keySerde,
        final Serde<GenericRow> valueSerde
    ) {
      this.name = name;
      this.keySerde = keySerde;
      this.valueSerde = valueSerde;
    }
  }

  private static final class Topic {
    // Topics that read from this topic
    private final List<Topic> receivers;
    // Drivers that take this topic as an input
    private final List<TopologyTestDriver> drivers;
    // Pipes flowing out of this topic
    private final List<TestInputTopic<GenericKey, GenericRow>> topicAsInput;
    // Pipe flowing into this topic.
    // For now, it's left as Optional because we don't support INSERT INTO...SELECT yet.
    private Optional<TestOutputTopic<GenericKey, GenericRow>> topicAsOutput;
    private final String name;

    private Topic(
        final String name
    ) {
      this.receivers = new ArrayList<>();
      this.drivers = new ArrayList<>();
      this.topicAsInput = new ArrayList<>();
      this.topicAsOutput = Optional.empty();
      this.name = name;
    }
  }

  // ----------------------------------------------------------------------------------

  private final Map<String, Topic> topics;
  private final ListMultimap<String, TestRecord<GenericKey, GenericRow>> topicCache;

  // this map indexes into the outputCache to track which records we've already
  // read - we don't need to worry about concurrent modification while iterating
  // because appends only happen at the end of the outputCache
  private final Map<String, Integer> assertPositions;

  public TestDriverPipeline() {
    topics = new HashMap<>();
    topicCache = ArrayListMultimap.create();
    assertPositions = new HashMap<>();
  }

  private TestInputTopic createInputTopic(final TopologyTestDriver driver, final TopicInfo topic) {
    return driver.createInputTopic(
        topic.name,
        topic.keySerde.serializer(),
        topic.valueSerde.serializer()
    );
  }

  private void replaceAllInputs(final TopicInfo topic) {
    topics.get(topic.name).topicAsInput.clear();
    topics.get(topic.name).drivers.forEach(driver ->
            topics.get(topic.name).topicAsInput.add(createInputTopic(driver, topic)));
  }

  public void addDdlTopic(final TopicInfo topic) {
    if (!topics.containsKey(topic.name)) {
      topics.put(topic.name, new Topic(topic.name));
    } else {
      replaceAllInputs(topic);
    }
  }

  public void addDriver(
      final TopologyTestDriver driver,
      final List<TopicInfo> inputTopics,
      final TopicInfo outputTopic
  ) {
    final TestOutputTopic output = driver.createOutputTopic(
        outputTopic.name,
        outputTopic.keySerde.deserializer(),
        outputTopic.valueSerde.deserializer());

    if (!topics.containsKey(outputTopic.name)) {
      topics.put(outputTopic.name, new Topic(outputTopic.name));
    } else {
      replaceAllInputs(outputTopic);
    }
    topics.get(outputTopic.name).topicAsOutput = Optional.of(output);

    for (TopicInfo inputTopic : inputTopics) {
      final TestInputTopic input = createInputTopic(driver, inputTopic);

      if (!topics.containsKey(inputTopic.name)) {
        topics.put(inputTopic.name, new Topic(inputTopic.name));
      }
      topics.get(inputTopic.name).topicAsInput.add(input);
      topics.get(inputTopic.name).receivers.add(topics.get(outputTopic.name));
      topics.get(inputTopic.name).drivers.add(driver);
    }
  }

  public void pipeInput(
      final String topic,
      final GenericKey key,
      final GenericRow value,
      final long timestampMs
  ) {
    pipeInput(topic, key, value, timestampMs, new HashSet<>(), topic);
  }

  private void pipeInput(
      final String topicName,
      final GenericKey key,
      final GenericRow value,
      final long timestampMs,
      final Set<String> loopDetection,
      final String path
  ) {
    final boolean added = loopDetection.add(topicName);
    if (!added) {
      throw new KsqlException("Detected illegal cycle in topology: " + path);
    }

    if (!topics.containsKey(topicName)) {
      throw new KsqlException("Cannot pipe input to unknown source: " + topicName);
    }

    final Topic topic = this.topics.get(topicName);

    if (topicName.equals(path)) {
      topicCache.put(topicName, new TestRecord(key, value, Instant.ofEpochMilli(timestampMs)));
    }

    for (final TestInputTopic input : topic.topicAsInput) {
      input.pipeInput(key, value, timestampMs);
    }

    for (final Topic receiver : topic.receivers) {
      for (final TestRecord<GenericKey, GenericRow> record :
          receiver.topicAsOutput.get().readRecordsToList()) {
        topicCache.put(receiver.name, record);
        if (topics.get(receiver.name).topicAsInput.size() > 0) {
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

  public List<TestRecord<GenericKey, GenericRow>> getAllRecordsForTopic(final String topic) {
    return topicCache.get(topic);
  }

  public Iterator<TestRecord<GenericKey, GenericRow>> getRecordsForTopic(final String topic) {
    return new Iterator<TestRecord<GenericKey, GenericRow>>() {
      @Override
      public boolean hasNext() {
        final int idx = assertPositions.getOrDefault(topic, 0);
        return topicCache.get(topic).size() > idx;
      }

      @Override
      public TestRecord<GenericKey, GenericRow> next() {
        final int idx = assertPositions.getOrDefault(topic, 0);
        assertPositions.put(topic, idx + 1);
        return topicCache.get(topic).get(idx);
      }
    };
  }

}
