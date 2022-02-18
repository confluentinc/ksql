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
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializedQueryResult;
import java.time.Instant;
import java.util.Optional;
import org.apache.kafka.streams.query.Position;

/**
 * Materialization of a table with a non-windowed key
 */
public interface StreamsMaterializedWindowedTable {

  /**
   * Get the values in table of the supplied {@code key}, where the window start time is within the
   * supplied {@code lower} and {@code upper} bounds.
   *
   * @param key the key to look up.
   * @param partition partition to limit the get to
   * @param windowStart the bounds on the window's start time.
   * @param windowEnd the bounds on the window's end time.
   * @return the rows for the key that exist within the range.
   */
  default KsMaterializedQueryResult<WindowedRow> get(
      GenericKey key,
      int partition,
      Range<Instant> windowStart,
      Range<Instant> windowEnd
  ) {
    return get(key, partition, windowStart, windowEnd, Optional.empty());
  }

  KsMaterializedQueryResult<WindowedRow> get(
      GenericKey key,
      int partition,
      Range<Instant> windowStart,
      Range<Instant> windowEnd,
      Optional<Position> position
  );

  /**
   * Get the values in table where the window start time is within the
   * supplied {@code lower} and {@code upper} bounds.
   *
   * @param partition partition to limit the get to
   * @param windowStart the bounds on the window's start time.
   * @param windowEnd the bounds on the window's end time.
   * @return the rows for the key that exist within the range.
   */
  default KsMaterializedQueryResult<WindowedRow> get(
      int partition,
      Range<Instant> windowStart,
      Range<Instant> windowEnd
  ) {
    return get(partition, windowStart, windowEnd, Optional.empty());
  }

  KsMaterializedQueryResult<WindowedRow> get(
      int partition,
      Range<Instant> windowStart,
      Range<Instant> windowEnd,
      Optional<Position> position
  );
}
