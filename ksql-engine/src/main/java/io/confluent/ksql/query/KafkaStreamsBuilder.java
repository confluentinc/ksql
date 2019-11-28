/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.query;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public interface KafkaStreamsBuilder {
  BuildResult buildKafkaStreams(StreamsBuilder builder, Map<String, Object> conf);

  class BuildResult {

    final Topology topology;
    final KafkaStreams kafkaStreams;

    public BuildResult(
        final Topology topology,
        final KafkaStreams kafkaStreams
    ) {
      this.topology = requireNonNull(topology, "topology");
      this.kafkaStreams = requireNonNull(kafkaStreams, "kafkaStreams");
    }
  }
}
