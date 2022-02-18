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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializedQueryResult;
import java.util.Optional;
import org.apache.kafka.streams.query.Position;

/**
 * Materialization of a table with a non-windowed key
 */
public interface StreamsMaterializedTable {

  /**
   * Get the value, if one exists, of the supplied {@code key}.
   *
   * @param key the key to look up.
   * @param partition partition to limit the get to
   * @return the value, if one is exists.
   */
  default KsMaterializedQueryResult<Row> get(
      GenericKey key,
      int partition
  ) {
    return get(key, partition, Optional.empty());
  }

  KsMaterializedQueryResult<Row> get(
      GenericKey key,
      int partition,
      Optional<Position> position
  );

  /**
   * Scan the table for rows
   *
   * @param partition partition to limit the get to
   * @return the rows.
   */
  default KsMaterializedQueryResult<Row> get(int partition) {
    return get(partition, Optional.empty());
  }

  KsMaterializedQueryResult<Row> get(int partition, Optional<Position> position);

  /**
   * RangeScan the table for rows
   *
   * @param partition partition to limit the get to
   * @param from first key in the range
   * @param to last key in range
   * @return the rows.
   */
  default KsMaterializedQueryResult<Row> get(
      int partition,
      GenericKey from,
      GenericKey to) {
    return get(partition, from, to, Optional.empty());
  }

  KsMaterializedQueryResult<Row> get(
      int partition,
      GenericKey from,
      GenericKey to,
      Optional<Position> position);
}
