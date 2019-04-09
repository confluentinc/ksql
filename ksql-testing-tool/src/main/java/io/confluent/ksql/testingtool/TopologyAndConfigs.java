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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class TopologyAndConfigs {

  final String topology;
  final Optional<String> schemas;
  final Optional<Map<String, String>> configs;

  TopologyAndConfigs(
      final String topology,
      final Optional<String> schemas,
      final Optional<Map<String, String>> configs
  ) {
    this.topology = Objects.requireNonNull(topology, "topology");
    this.schemas = Objects.requireNonNull(schemas, "schemas");
    this.configs = Objects.requireNonNull(configs, "configs");
  }
}