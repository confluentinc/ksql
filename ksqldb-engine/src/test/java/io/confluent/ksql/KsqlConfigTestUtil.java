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

package io.confluent.ksql;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestUtils;

public final class KsqlConfigTestUtil {

  private static final Supplier<ImmutableMap<String, Object>> BASE_CONFIG_SUPPLIER =
      () -> ImmutableMap.of(
          "commit.interval.ms", 0,
          "cache.max.bytes.buffering", 0,
          "auto.offset.reset", "earliest",
          StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()
      );

  private KsqlConfigTestUtil() {}

  @SuppressFBWarnings(value = "MS_EXPOSE_REP", justification = "BASE_CONFIG is ImmutableMap")
  public static Map<String, Object> baseTestConfig() {
    return BASE_CONFIG_SUPPLIER.get();
  }

  public static KsqlConfig create(final EmbeddedSingleNodeKafkaCluster kafkaCluster) {
    return create(kafkaCluster, Collections.emptyMap());
  }

  public static KsqlConfig create(
      final EmbeddedSingleNodeKafkaCluster kafkaCluster,
      final Map<String, Object> additionalConfig
  ) {
    final ImmutableMap<String, Object> config = ImmutableMap.<String, Object>builder()
        .putAll(kafkaCluster.getClientProperties())
        .putAll(additionalConfig)
        .build();
    return create(kafkaCluster.bootstrapServers(), config);
  }

  public static KsqlConfig create(
      final String kafkaBootstrapServers
  ) {
    return create(kafkaBootstrapServers, ImmutableMap.of());
  }

  public static KsqlConfig create(
      final String kafkaBootstrapServers,
      final Map<String, Object> additionalConfig
  ) {
    final Map<String, Object> config = new HashMap<>(BASE_CONFIG_SUPPLIER.get());
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    config.putAll(additionalConfig);
    return new KsqlConfig(config);
  }
}
