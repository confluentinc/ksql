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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.execution.streams.materialization.TableRowValidation.Validator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;
import java.util.Optional;

public final class Row implements TableRow {

  private final LogicalSchema schema;
  private final GenericKey key;
  private final GenericRow value;
  private final long rowTime;
  private final Validator validator;

  public static Row of(
      final LogicalSchema schema,
      final GenericKey key,
      final GenericRow value,
      final long rowTime
  ) {
    return new Row(schema, key, value, rowTime, TableRowValidation::validate);
  }

  @VisibleForTesting
  Row(
      final LogicalSchema schema,
      final GenericKey key,
      final GenericRow value,
      final long rowTime,
      final Validator validator
  ) {
    this.schema = requireNonNull(schema, "schema");
    this.key = requireNonNull(key, "key");
    this.value = requireNonNull(value, "value");
    this.rowTime = rowTime;
    this.validator = requireNonNull(validator, "validator");

    validator.validate(schema, key, value);
  }

  @Override
  public LogicalSchema schema() {
    return schema;
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
    return Optional.empty();
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "should be mutable")
  public GenericRow value() {
    return value;
  }

  @Override
  public Row withValue(
      final GenericRow newValue,
      final LogicalSchema newSchema
  ) {
    return new Row(
        newSchema,
        key,
        newValue,
        rowTime,
        validator
    );
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Row that = (Row) o;
    return Objects.equals(schema, that.schema)
        && Objects.equals(key, that.key)
        && Objects.equals(value, that.value)
        && Objects.equals(rowTime, that.rowTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, key, value, rowTime);
  }

  @Override
  public String toString() {
    return "Row{"
        + "key=" + key
        + ", value=" + value
        + ", rowTime=" + rowTime
        + ", schema=" + schema
        + '}';
  }
}
