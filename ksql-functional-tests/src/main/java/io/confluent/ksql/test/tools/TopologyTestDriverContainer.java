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

import io.confluent.ksql.metastore.model.KsqlTopic;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.TopologyTestDriver;

public final class TopologyTestDriverContainer {

  private final TopologyTestDriver topologyTestDriver;
  private final List<KsqlTopic> sourceKsqlTopics;
  private final KsqlTopic sinkKsqlTopic;

  private TopologyTestDriverContainer(
      final TopologyTestDriver topologyTestDriver,
      final List<KsqlTopic> sourceKsqlTopics,
      final KsqlTopic sinkKsqlTopic) {
    this.topologyTestDriver = topologyTestDriver;
    this.sourceKsqlTopics = sourceKsqlTopics;
    this.sinkKsqlTopic = sinkKsqlTopic;
  }

  public static TopologyTestDriverContainer of(
      final TopologyTestDriver topologyTestDriver,
      final List<KsqlTopic> sourceKsqlTopics,
      final KsqlTopic sinkKsqlTopic) {
    Objects.requireNonNull(topologyTestDriver, "topologyTestDriver");
    Objects.requireNonNull(sourceKsqlTopics, "sourceKsqlTopics");
    Objects.requireNonNull(sinkKsqlTopic, "sinkKsqlTopic");
    return new TopologyTestDriverContainer(topologyTestDriver, sourceKsqlTopics, sinkKsqlTopic);
  }

  TopologyTestDriver getTopologyTestDriver() {
    return topologyTestDriver;
  }

  List<KsqlTopic> getSourceKsqlTopics() {
    return sourceKsqlTopics;
  }

  public KsqlTopic getSinkKsqlTopic() {
    return sinkKsqlTopic;
  }
}