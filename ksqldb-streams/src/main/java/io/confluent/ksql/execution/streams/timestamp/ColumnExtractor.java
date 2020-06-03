/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.streams.timestamp;

import io.confluent.ksql.GenericRow;

/**
 * Extracts a column from the ConsumerRecord
 */
interface ColumnExtractor {

  /**
   * Extract a key or value column from the supplied {@code key} or {@code value}.
   *
   * @param key the key - either a {@link org.apache.kafka.connect.data.Struct}, or a {@link
   * org.apache.kafka.streams.kstream.Windowed} Struct.
   * @param value the value.
   * @return the extracted column value.
   */
  Object extract(Object key, GenericRow value);
}
