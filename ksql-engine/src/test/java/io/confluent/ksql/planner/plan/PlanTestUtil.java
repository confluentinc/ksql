/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.ksql.planner.plan;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


public class PlanTestUtil {

  static final String TRANSFORM_NODE = "KSTREAM-TRANSFORMVALUES-0000000002";
  static final String SOURCE_NODE = "KSTREAM-SOURCE-0000000000";
  static final String MAPVALUES_NODE = "KSTREAM-MAPVALUES-0000000001";

  static TopologyDescription.Node getNodeByName(final Topology topology, final String nodeName) {
    final TopologyDescription description = topology.describe();
    final Set<TopologyDescription.Subtopology> subtopologies = description.subtopologies();
    List<TopologyDescription.Node> nodes = subtopologies.stream().flatMap(subtopology -> subtopology.nodes().stream()).collect(Collectors.toList());
    final Map<String, List<TopologyDescription.Node>> nodesByName = nodes.stream().collect(Collectors.groupingBy(TopologyDescription.Node::name));
    return nodesByName.get(nodeName).get(0);
  }

  static void verifyProcessorNode(final TopologyDescription.Processor node,
                                   final List<String> expectedPredecessors,
                                   final List<String> expectedSuccessors) {
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    final List<String> predecessors = node.predecessors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(predecessors, equalTo(expectedPredecessors));
    assertThat(successors, equalTo(expectedSuccessors));
  }
}
