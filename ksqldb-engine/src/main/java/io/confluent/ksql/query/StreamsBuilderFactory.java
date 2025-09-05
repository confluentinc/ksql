/*
 * Copyright 2025 Confluent Inc.
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

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;

/**
 * Factory for creating configured instances of {@link StreamsBuilder}.
 * This is a temporary fix. The permanent fix will be made in processValues
 */
public final class StreamsBuilderFactory {

  // config that will be passed to fix process values behaviour
  private static final boolean ENABLE_PROCESS_PROCESSVALUE_FIX = true;
  // dummy configs to pass the init validations
  private static final String DUMMY_APPLICATION_ID = "dummy_id";
  private static final String DUMMY_BOOTSTRAP_SERVERS = "dummy_servers";

  private StreamsBuilderFactory() {
  }

  public static StreamsBuilder create() {
    final Map<String, Object> initStreamsProperties = new HashMap<>();
    initStreamsProperties.put(
        TopologyConfig.InternalConfig.ENABLE_PROCESS_PROCESSVALUE_FIX,
        ENABLE_PROCESS_PROCESSVALUE_FIX
    );
    initStreamsProperties.put(
        StreamsConfig.APPLICATION_ID_CONFIG,
        DUMMY_APPLICATION_ID);
    initStreamsProperties.put(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        DUMMY_BOOTSTRAP_SERVERS);
    return new StreamsBuilder(new TopologyConfig(new StreamsConfig(initStreamsProperties)));
  }
}


