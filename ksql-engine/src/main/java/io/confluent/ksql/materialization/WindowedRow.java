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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.materialization.TableRowValidation.Validator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;

@Immutable
public final class WindowedRow implements TableRow {

  private final LogicalSchema schema;
  private final Window window;
  private final Struct key;
  private final GenericRow value;
  private final Validator validator;

  public static WindowedRow of(
      final LogicalSchema schema,
      final Struct key,
      final Window window,
      final GenericRow value
  ) {
    return new WindowedRow(schema, key, window, value, TableRowValidation::validate);
  }

  @VisibleForTesting
  WindowedRow(
      final LogicalSchema schema,
      final Struct key,
      final Window window,
      final GenericRow value,
      final Validator validator
  ) {
    this.schema = requireNonNull(schema, "schema");
    this.key = requireNonNull(key, "key");
    this.window = requireNonNull(window, "window");
    this.value = requireNonNull(value, "value");
    this.validator = requireNonNull(validator, "validator");

    validator.validate(schema, key, value);
  }

  @Override
  public LogicalSchema schema() {
    return schema;
  }

  @Override
  public Struct key() {
    return key;
  }

  @Override
  public Optional<Window> window() {
    return Optional.of(window);
  }

  @Override
  public GenericRow value() {
    return value;
  }

  WindowedRow withValue(
      final GenericRow newValue,
      final LogicalSchema newSchema
  ) {
    return new WindowedRow(
        newSchema,
        key,
        window,
        newValue,
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
    final WindowedRow that = (WindowedRow) o;
    return Objects.equals(schema, that.schema)
        && Objects.equals(key, that.key)
        && Objects.equals(window, that.window)
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, window, value, schema);
  }

  @Override
  public String toString() {
    return "WindowedRow{"
        + "key=" + key
        + ", window=" + window
        + ", value=" + value
        + ", schema=" + schema
        + '}';
  }
}
