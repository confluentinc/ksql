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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.PushOffsetRange;
import java.util.Objects;
import java.util.Optional;

public final class QueryRowImpl implements QueryRow {

  private final LogicalSchema logicalSchema;
  private final long rowTime;
  private final GenericKey key;
  private final Optional<Window> window;
  private final GenericRow value;

  public static QueryRowImpl of(
      final LogicalSchema logicalSchema,
      final GenericKey key,
      final Optional<Window> window,
      final GenericRow value,
      final long rowTime
  ) {
    return new QueryRowImpl(logicalSchema, key, window, value, rowTime);
  }

  private QueryRowImpl(
      final LogicalSchema logicalSchema,
      final GenericKey key,
      final Optional<Window> window,
      final GenericRow value,
      final long rowTime
  ) {
    this.logicalSchema = logicalSchema;
    this.rowTime = rowTime;
    this.key = key;
    this.window = window;
    this.value = value;
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

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  @Override
  public GenericRow value() {
    return value;
  }

  @Override
  public Optional<PushOffsetRange> getOffsetRange() {
    return Optional.empty();
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
        && Objects.equals(window, that.window)
        && Objects.equals(value, that.value)
        && Objects.equals(rowTime, that.rowTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logicalSchema, key, window, value, rowTime);
  }

  @Override
  public String toString() {
    return "QueryRowImpl{"
        + "logicalSchema=" + logicalSchema
        + ", key=" + key
        + ", window=" + window
        + ", value=" + value
        + ", rowTime=" + rowTime
        + '}';
  }
}
