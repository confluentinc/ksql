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

package io.confluent.ksql.execution.streams.materialization;

import com.google.common.collect.Range;
import java.time.Instant;
import java.util.List;
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
   * @param partition partition to limit the get to
   * @param windowStart the bounds on the window's start time.
   * @param windowEnd the bounds on the window's end time.
   * @return the rows for the key that exist within the range.
   */
  List<WindowedRow> get(Struct key, int partition, Range<Instant> windowStart,
      Range<Instant> windowEnd);
}
