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

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.tools.test.model.Topic;
import io.confluent.ksql.util.Pair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

public final class TopologyTestDriverContainer {

  private final TopologyTestDriver topologyTestDriver;
  private final Map<String, TestInputTopic<byte[], byte[]>> sourceTopics;
  private final Optional<String> sinkTopicName;
  private final Map<String, TestOutputTopic<byte[], byte[]>> sinkTopics = new HashMap<>();

  public static TopologyTestDriverContainer of(
      final TopologyTestDriver topologyTestDriver,
      final List<Topic> sourceTopics,
      final Optional<Topic> sinkTopic
  ) {
    return new TopologyTestDriverContainer(
        topologyTestDriver,
        sourceTopics,
        sinkTopic
    );
  }

  private TopologyTestDriverContainer(
      final TopologyTestDriver topologyTestDriver,
      final List<Topic> sourceTopics,
      final Optional<Topic> sinkTopic
  ) {
    this.topologyTestDriver = requireNonNull(topologyTestDriver, "topologyTestDriver");
    requireNonNull(sourceTopics, "sourceTopics");
    this.sourceTopics = sourceTopics
        .stream()
        .map(topic -> Pair.of(
            topic.getName(),
            topologyTestDriver.createInputTopic(
                topic.getName(),
                new ByteArraySerializer(),
                new ByteArraySerializer())
            )
        )
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    sinkTopicName = requireNonNull(sinkTopic, "sinkTopic").map(Topic::getName);
    sinkTopicName.ifPresent(topicName -> sinkTopics.put(
        topicName,
        topologyTestDriver.createOutputTopic(
            topicName,
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
        )
    ));
  }

  TopologyTestDriver getTopologyTestDriver() {
    return topologyTestDriver;
  }

  public TestInputTopic<byte[], byte[]> getSourceTopic(final String topicName) {
    return sourceTopics.get(topicName);
  }

  public TestOutputTopic<byte[], byte[]> getSinkTopic(final String topicName) {
    return sinkTopics.computeIfAbsent(
        topicName,
        t -> topologyTestDriver.createOutputTopic(
            t,
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
        )
    );
  }

  public Optional<String> getSinkTopicName() {
    return sinkTopicName;
  }

  public Set<String> getSourceTopicNames() {
    return sourceTopics.keySet();
  }

  public void close() {
    topologyTestDriver.close();
  }
}