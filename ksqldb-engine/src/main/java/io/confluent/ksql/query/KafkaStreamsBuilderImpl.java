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

import io.confluent.ksql.properties.PropertiesUtil;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;

public class KafkaStreamsBuilderImpl implements KafkaStreamsBuilder {
  private final KafkaClientSupplier clientSupplier;

  KafkaStreamsBuilderImpl(final KafkaClientSupplier clientSupplier) {
    this.clientSupplier = Objects.requireNonNull(clientSupplier, "clientSupplier");
  }

  public KafkaStreams build(final Topology topology, final Map<String, Object> conf) {
    return new KafkaStreams(topology, PropertiesUtil.asProperties(conf), clientSupplier);
  }

  public KafkaStreamsNamedTopologyWrapper buildNamedTopologyWrapper(
          final Map<String, Object> conf
  ) {
    return new KafkaStreamsNamedTopologyWrapper(
        PropertiesUtil.asProperties(conf),
        clientSupplier
    );
  }
}
