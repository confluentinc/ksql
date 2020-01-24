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

package io.confluent.ksql.metastore.model;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Objects;
import java.util.Optional;

/**
 * Pojo that holds the details of a source's key field.
 */
@Immutable
public final class KeyField {

  private static final KeyField NONE = KeyField.of(Optional.empty());

  private final Optional<ColumnRef> keyField;

  public static KeyField none() {
    return NONE;
  }

  public static KeyField of(final ColumnRef keyField) {
    return new KeyField(Optional.of(keyField));
  }

  public static KeyField of(final Optional<ColumnRef> keyField) {
    return new KeyField(keyField);
  }

  private KeyField(
      final Optional<ColumnRef> keyField
  ) {
    this.keyField = Objects.requireNonNull(keyField, "keyField");
  }

  /**
   * Validate the key field, if set, is contained within the supplied {@code schema}.
   *
   * @param schema the associated schema that the key should be present in.
   * @return self, to allow fluid syntax.
   * @throws IllegalArgumentException if the key is not within the supplied schema.
   */
  public KeyField validateKeyExistsIn(final LogicalSchema schema) {
    resolve(schema);
    return this;
  }

  public Optional<ColumnRef> ref() {
    return keyField;
  }

  /**
   * Resolve this {@code KeyField} to the specific key {@code Column} to use.
   *
   * <p>The method inspects the supplied {@code ksqlConfig} to determine if the new or legacy
   * key field should be returned.
   *
   * <p>The new key field is obtained from the supplied {@code schema} using the instance's
   * {@code keyField} field as the column name.
   *
   * <p>The legacy key field is obtained from the instance's {@code legacyKeyField}.
   *
   * @param schema the schema to use when resolving new key fields.
   * @return the resolved key column, or {@link Optional#empty()} if no key field is set.
   * @throws IllegalArgumentException if new key field is required but not available in the schema.
   */
  public Optional<Column> resolve(final LogicalSchema schema) {
    final Optional<Column> resolved = keyField
        .map(colRef -> schema.findValueColumn(colRef)
            .orElseThrow(() -> new IllegalArgumentException(
                "Invalid key field, not found in schema: "
                    + colRef.toString(FormatOptions.noEscape()))));

    resolved.ifPresent(col -> throwOnTypeMismatch(schema, col));

    return resolved;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final KeyField that = (KeyField) o;
    return Objects.equals(keyField, that.keyField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyField);
  }

  @Override
  public String toString() {
    return "KeyField(" + keyField + ')';
  }

  private static void throwOnTypeMismatch(final LogicalSchema schema, final Column keyField) {
    if (schema.key().size() != 1) {
      throw new UnsupportedOperationException("Only single key column supported");
    }

    final Column keyCol = schema.key().get(0);
    final SqlType keyType = keyCol.type();
    final SqlType keyFieldType = keyField.type();

    if (!keyType.equals(keyFieldType)) {
      throw new IllegalArgumentException("The type of the KEY field defined in the WITH clause "
          + "does not match the type of the actual row key column."
          + System.lineSeparator()
          + "KEY column in WITH clause: " + keyField
          + System.lineSeparator()
          + "actual key column:" + keyCol
      );
    }
  }
}
