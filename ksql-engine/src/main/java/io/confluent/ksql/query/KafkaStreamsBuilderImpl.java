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

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public class KafkaStreamsBuilderImpl implements KafkaStreamsBuilder {

  private final KafkaClientSupplier clientSupplier;

  KafkaStreamsBuilderImpl(final KafkaClientSupplier clientSupplier) {
    this.clientSupplier = Objects.requireNonNull(clientSupplier, "clientSupplier");
  }

  @Override
  public BuildResult buildKafkaStreams(
      final StreamsBuilder builder,
      final Map<String, Object> conf
  ) {
    final Properties props = new Properties();
    props.putAll(conf);
    final Topology topology = builder.build(props);
    final KafkaStreams kafkaStreams = new KafkaStreams(topology, props, clientSupplier);
    return new BuildResult(topology, kafkaStreams);
  }
}
