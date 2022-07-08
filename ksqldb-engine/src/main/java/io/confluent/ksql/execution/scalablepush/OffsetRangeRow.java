/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.scalablepush;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.PushOffsetRange;
import java.util.Optional;

/**
 * A row that represents an offset range for push queries. These are sent after batches of data
 * so that the coordinator node can verify that no gaps have been detected.
 */
public class OffsetRangeRow implements QueryRow {

  private static final LogicalSchema EMPTY_SCHEMA = LogicalSchema.builder().build();
  private final long rowTime;
  final PushOffsetRange offsetRange;

  public static OffsetRangeRow of(
      final long rowTime,
      final PushOffsetRange offsetRange
  ) {
    return new OffsetRangeRow(rowTime, offsetRange);
  }

  OffsetRangeRow(
      final long rowTime,
      final PushOffsetRange offsetRange
  ) {
    this.rowTime = rowTime;
    this.offsetRange = offsetRange;
  }

  @Override
  public LogicalSchema schema() {
    return EMPTY_SCHEMA;
  }

  @Override
  public long rowTime() {
    return rowTime;
  }

  @Override
  public GenericKey key() {
    return GenericKey.genericKey();
  }

  @Override
  public Optional<Window> window() {
    return Optional.empty();
  }

  @Override
  public GenericRow value() {
    return GenericRow.genericRow();
  }

  @Override
  public Optional<PushOffsetRange> getOffsetRange() {
    return Optional.of(offsetRange);
  }
}
