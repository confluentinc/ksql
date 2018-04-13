/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.util.timestamp;

import java.util.Map;

import io.confluent.ksql.util.KsqlConfig;

public interface TimestampExtractionPolicy {
  /**
   * Apply the timestamp extraction policy to the newStreamProperties
   * @param config the KsqlConfig to extract any additional configuration from
   * @param newStreamProperties the properties that will be used to create the new stream
   */
  void applyTo(final KsqlConfig config, final Map<String, Object> newStreamProperties);

  default String timestampField() {
    return null;
  }
}
