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
import java.util.Optional;

public class OffsetsRow implements QueryRow {

  private static final LogicalSchema EMPTY_SCHEMA = LogicalSchema.builder().build();
  private final long rowTime;
  private final Optional<List<Long>> startOffsets;
  private final List<Long> offsets;

  public static OffsetsRow of(
      final long rowTime,
      final Optional<List<Long>> startOffsets,
      final List<Long> offsets
  ) {
    return new OffsetsRow(rowTime, startOffsets, offsets);
  }

  OffsetsRow(
      final long rowTime,
      final Optional<List<Long>> startOffsets,
      final List<Long> offsets
  ) {
    this.rowTime = rowTime;
    this.startOffsets = startOffsets;
    this.offsets = offsets;
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
  public Optional<List<Long>> getStartOffsets() {
    return startOffsets;
  }

  @Override
  public Optional<List<Long>> getOffsets() {
    return Optional.of(offsets);
  }
}
