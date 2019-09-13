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

package io.confluent.ksql.materialization;

import io.confluent.ksql.GenericRow;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;

/**
 * Materialization of a table with a windowed key
 */
public interface MaterializedWindowedTable {

  /**
   * Get the values in table of the supplied {@code key}, where the window start time is within
   * the supplied {@code lower} and {@code upper} bounds.
   *
   * @param key the key to look up.
   * @param lower the lower bound on the window's start time, (inclusive).
   * @param upper the upper bound on the window's start time, (inclusive).
   * @return the value, if one is exists.
   */
  Map<Window, GenericRow> get(Struct key, Instant lower, Instant upper);
}
