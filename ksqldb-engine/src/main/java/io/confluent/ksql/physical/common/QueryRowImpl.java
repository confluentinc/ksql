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

package io.confluent.ksql.physical.common;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class QueryRowImpl implements QueryRow {

  private final LogicalSchema logicalSchema;
  private final long rowTime;
  private final GenericKey key;
  private final Optional<Window> window;
  private final GenericRow value;
  private final Optional<List<Long>> startOffsets;
  private final Optional<List<Long>> offsets;

  public static QueryRowImpl of(
      final LogicalSchema logicalSchema,
      final GenericKey key,
      final Optional<Window> window,
      final GenericRow value,
      final long rowTime
  ) {
    return new QueryRowImpl(logicalSchema, key, window, value, rowTime);
  }

  public static QueryRowImpl of(
      final LogicalSchema logicalSchema,
      final GenericKey key,
      final Optional<Window> window,
      final GenericRow value,
      final long rowTime,
      final Optional<List<Long>> startOffsets,
      final Optional<List<Long>> offsets
  ) {
    return new QueryRowImpl(
        logicalSchema, key, window, value, rowTime, startOffsets, offsets);
  }

  private QueryRowImpl(
      final LogicalSchema logicalSchema,
      final GenericKey key,
      final Optional<Window> window,
      final GenericRow value,
      final long rowTime
  ) {
    this(logicalSchema, key, window, value, rowTime, Optional.empty(), Optional.empty());
  }

  private QueryRowImpl(
      final LogicalSchema logicalSchema,
      final GenericKey key,
      final Optional<Window> window,
      final GenericRow value,
      final long rowTime,
      final Optional<List<Long>> startOffsets,
      final Optional<List<Long>> offsets
  ) {
    this.logicalSchema = logicalSchema;
    this.rowTime = rowTime;
    this.key = key;
    this.window = window;
    this.value = value;
    this.startOffsets = startOffsets;
    this.offsets = offsets;
  }

  @Override
  public LogicalSchema schema() {
    return logicalSchema;
  }

  @Override
  public long rowTime() {
    return rowTime;
  }

  @Override
  public GenericKey key() {
    return key;
  }

  @Override
  public Optional<Window> window() {
    return window;
  }

  @Override
  public GenericRow value() {
    return value;
  }

  @Override
  public Optional<List<Long>> getStartOffsets() {
    return startOffsets;
  }

  @Override
  public Optional<List<Long>> getOffsets() {
    return offsets;
  }


  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final QueryRowImpl that = (QueryRowImpl) o;
    return Objects.equals(logicalSchema, that.logicalSchema)
        && Objects.equals(key, that.key)
        && Objects.equals(value, that.value)
        && Objects.equals(rowTime, that.rowTime)
        && Objects.equals(startOffsets, that.startOffsets)
        && Objects.equals(offsets, that.offsets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logicalSchema, key, value, rowTime, startOffsets, offsets);
  }
}
