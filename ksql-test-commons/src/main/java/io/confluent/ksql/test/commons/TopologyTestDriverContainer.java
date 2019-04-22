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

package io.confluent.ksql.test.commons;

import io.confluent.ksql.metastore.model.KsqlTopic;
import java.util.Set;
import org.apache.kafka.streams.TopologyTestDriver;

public final class TopologyTestDriverContainer {

  private final TopologyTestDriver topologyTestDriver;
  private final Set<KsqlTopic> sourceKsqlTopics;
  private final Set<KsqlTopic> sinkKsqlTopics;

  private TopologyTestDriverContainer(
      final TopologyTestDriver topologyTestDriver,
      final Set<KsqlTopic> sourceKsqlTopics,
      final Set<KsqlTopic> sinkKsqlTopics) {
    this.topologyTestDriver = topologyTestDriver;
    this.sourceKsqlTopics = sourceKsqlTopics;
    this.sinkKsqlTopics = sinkKsqlTopics;
  }

  public static TopologyTestDriverContainer of(
      final TopologyTestDriver topologyTestDriver,
      final Set<KsqlTopic> sourceKsqlTopics,
      final Set<KsqlTopic> sinkKsqlTopics) {
    return new TopologyTestDriverContainer(topologyTestDriver, sourceKsqlTopics, sinkKsqlTopics);
  }

  TopologyTestDriver getTopologyTestDriver() {
    return topologyTestDriver;
  }

  Set<KsqlTopic> getSourceKsqlTopics() {
    return sourceKsqlTopics;
  }

  public Set<KsqlTopic> getSinkKsqlTopics() {
    return sinkKsqlTopics;
  }
}