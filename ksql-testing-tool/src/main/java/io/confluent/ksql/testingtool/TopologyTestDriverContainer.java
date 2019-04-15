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

package io.confluent.ksql.testingtool;

import java.util.Set;
import org.apache.kafka.streams.TopologyTestDriver;

public class TopologyTestDriverContainer {

  private final TopologyTestDriver topologyTestDriver;
  private final Set<String> sourceTopics;
  private final Set<String> sinkTopics;

  public TopologyTestDriverContainer(
      final TopologyTestDriver topologyTestDriver,
      final Set<String> sourceTopics,
      final Set<String> sinkTopics) {
    this.topologyTestDriver = topologyTestDriver;
    this.sourceTopics = sourceTopics;
    this.sinkTopics = sinkTopics;
  }

  public TopologyTestDriver getTopologyTestDriver() {
    return topologyTestDriver;
  }

  public Set<String> getSourceTopics() {
    return sourceTopics;
  }

  public Set<String> getSinkTopics() {
    return sinkTopics;
  }
}
