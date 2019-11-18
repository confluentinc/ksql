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

package io.confluent.ksql.config;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Set;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Hard coded list of known immutable properties
 */
public final class ImmutableProperties {

  private static final Set<String> IMMUTABLE_PROPERTIES = ImmutableSet.<String>builder()
      .add(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)
      .add(KsqlConfig.KSQL_EXT_DIR)
      .add(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG)
      .add(KsqlConfig.KSQL_QUERY_PULL_ENABLE_CONFIG)
      .addAll(KsqlConfig.SSL_CONFIG_NAMES)
      .build();

  private ImmutableProperties() {
  }

  public static Set<String> getImmutableProperties() {
    return IMMUTABLE_PROPERTIES;
  }
}
