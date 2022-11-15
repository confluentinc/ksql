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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.TopologyTestDriver;

public final class TopologyTestDriverContainer {

  private final TopologyTestDriver topologyTestDriver;
  private final List<Topic> sourceTopics;
  private final Topic sinkTopic;
  private final Set<String> sourceTopicNames;

  public static TopologyTestDriverContainer of(
      final TopologyTestDriver topologyTestDriver,
      final List<Topic> sourceTopics,
      final Topic sinkTopic
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
      final Topic sinkTopic
  ) {
    this.topologyTestDriver = requireNonNull(topologyTestDriver, "topologyTestDriver");
    this.sourceTopics = requireNonNull(sourceTopics, "sourceTopics");
    this.sinkTopic = requireNonNull(sinkTopic, "sinkTopic");
    this.sourceTopicNames = sourceTopics.stream().map(Topic::getName).collect(Collectors.toSet());
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