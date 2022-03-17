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

package io.confluent.ksql.execution.common;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.PushOffsetRange;
import java.util.Objects;
import java.util.Optional;

public class OffsetsRow implements QueryRow {

  private static final LogicalSchema EMPTY_SCHEMA = LogicalSchema.builder().build();
  private final long rowTime;
  private final PushOffsetRange pushOffsetRange;

  public static OffsetsRow of(
      final long rowTime,
      final PushOffsetRange pushOffsetRange
  ) {
    return new OffsetsRow(rowTime, pushOffsetRange);
  }

  OffsetsRow(
      final long rowTime,
      final PushOffsetRange pushOffsetRange
  ) {
    this.rowTime = rowTime;
    this.pushOffsetRange = pushOffsetRange;
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
    return Optional.of(pushOffsetRange);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final OffsetsRow that = (OffsetsRow) o;
    return Objects.equals(pushOffsetRange, that.pushOffsetRange)
        && Objects.equals(rowTime, that.rowTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pushOffsetRange, rowTime);
  }

  @Override
  public String toString() {
    return "OffsetsRow{"
        + "pushOffsetRange=" + pushOffsetRange
        + ", rowTime=" + rowTime
        + '}';
  }
}
