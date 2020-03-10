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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.TopologyTestDriver;

public final class TopologyTestDriverContainer {

  private final TopologyTestDriver topologyTestDriver;
  private final List<Topic> sourceTopics;
  private final Topic sinkTopic;
  private final Set<String> sourceTopicNames;

  private TopologyTestDriverContainer(
      final TopologyTestDriver topologyTestDriver,
      final List<Topic> sourceTopics,
      final Topic sinkTopic) {
    this.topologyTestDriver = topologyTestDriver;
    this.sourceTopics = sourceTopics;
    this.sinkTopic = sinkTopic;
    this.sourceTopicNames = sourceTopics.stream().map(Topic::getName).collect(Collectors.toSet());
  }

  public static TopologyTestDriverContainer of(
      final TopologyTestDriver topologyTestDriver,
      final List<Topic> sourceTopics,
      final Topic sinkTopic) {
    Objects.requireNonNull(topologyTestDriver, "topologyTestDriver");
    Objects.requireNonNull(sourceTopics, "sourceTopics");
    Objects.requireNonNull(sinkTopic, "sinkTopic");
    return new TopologyTestDriverContainer(
        topologyTestDriver,
        sourceTopics,
        sinkTopic);
  }

  TopologyTestDriver getTopologyTestDriver() {
    return topologyTestDriver;
  }

  public List<Topic> getSourceTopics() {
    return sourceTopics;
  }

  public Topic getSinkTopic() {
    return sinkTopic;
  }

  public Set<String> getSourceTopicNames() {
    return sourceTopicNames;
  }
}