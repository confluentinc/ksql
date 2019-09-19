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

package io.confluent.ksql.schema.ksql;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Objects;
import java.util.Optional;

/**
 * A named field within KSQL schema types.
 */
@Immutable
public final class Column {

  private final ColumnRef ref;
  private final SqlType type;

  /**
   * @param name the name of the field.
   * @param type the type of the field.
   * @return the immutable field.
   */
  public static Column of(final ColumnName name, final SqlType type) {
    return new Column(ColumnRef.of(name), type);
  }

  /**
   * @param source the name of the source of the field.
   * @param name the name of the field.
   * @param type the type of the field.
   * @return the immutable field.
   */
  public static Column of(final SourceName source, final ColumnName name, final SqlType type) {
    return new Column(ColumnRef.of(source, name), type);
  }

  /**
   * @param source the name of the source of the field.
   * @param name the name of the field.
   * @param type the type of the field.
   * @return the immutable field.
   */
  public static Column of(
      final Optional<SourceName> source,
      final ColumnName name,
      final SqlType type
  ) {
    return new Column(ColumnRef.of(source, name), type);
  }

  private Column(final ColumnRef ref, final SqlType type) {
    this.ref = Objects.requireNonNull(ref, "name");
    this.type = Objects.requireNonNull(type, "type");
  }

  /**
   * @return the fully qualified field name.
   */
  public String fullName() {
    return ref.aliasedFieldName();
  }

  /**
   * @return the source of the Column
   */
  public Optional<SourceName> source() {
    return ref.qualifier();
  }

  /**
   * @return the name of the field, without any source / alias.
   */
  public ColumnName name() {
    return ref.name();
  }

  /**
   * @return the type of the field.
   */
  public SqlType type() {
    return type;
  }

  /**
   * Create a new Field that matches the current, but with the supplied {@code source}.
   *
   * @param source the source to set of the new field.
   * @return the new field.
   */
  public Column withSource(final SourceName source) {
    return new Column(ref.withSource(source), type);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Column that = (Column) o;
    return Objects.equals(ref, that.ref)
        && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ref, type);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    final String fmtType = type.toString(formatOptions);

    return ref.toString(formatOptions) + " " + fmtType;
  }
}
