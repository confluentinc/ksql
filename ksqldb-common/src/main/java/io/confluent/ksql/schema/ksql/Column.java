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
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.DecimalUtil;
import java.util.Objects;
import java.util.Optional;

/**
 * A named field within KSQL schema types.
 */
@Immutable
public final class Column implements SimpleColumn {

  // The order of the enum defines the order of precedence of {@code LogicalScheam.findColumn}.
  public enum Namespace {
    KEY,
    VALUE,
    HEADERS
  }

  private final ColumnName name;
  private final SqlType type;
  private final Namespace namespace;
  private final int index;
  private final Optional<String> headerKey;

  /**
   * @param name the name of the field.
   * @param type the type of the field.
   * @param namespace the namespace of the field.
   * @param indexWithinNamespace the column index within the namespace.
   *
   * @return the immutable field.
   */
  public static Column of(
      final ColumnName name,
      final SqlType type,
      final Namespace namespace,
      final int indexWithinNamespace
  ) {
    return new Column(name, type, namespace, indexWithinNamespace, Optional.empty());
  }

  /**
   * @param name the name of the field.
   * @param type the type of the field.
   * @param namespace the namespace of the field.
   * @param indexWithinNamespace the column index within the namespace.
   * @param headerKey the key to extract from the header, if this is a header column
   *
   * @return the immutable field.
   */
  public static Column of(
      final ColumnName name,
      final SqlType type,
      final Namespace namespace,
      final int indexWithinNamespace,
      final Optional<String> headerKey
  ) {
    return new Column(name, type, namespace, indexWithinNamespace, headerKey);
  }

  private Column(
      final ColumnName name,
      final SqlType type,
      final Namespace namespace,
      final int index,
      final Optional<String> headerKey
  ) {
    this.name = Objects.requireNonNull(name, "name");
    this.type = Objects.requireNonNull(type, "type");
    this.namespace = Objects.requireNonNull(namespace, "namespace");
    this.index = index;
    this.headerKey = Objects.requireNonNull(headerKey, "headerKey");

    if (index < 0) {
      throw new IllegalArgumentException("Invalid column index: " + index);
    }
  }

  /**
   * @return the name of the field, without any source / alias.
   */
  public ColumnName name() {
    return name;
  }

  /**
   * @return the type of the field.
   */
  @Override
  public SqlType type() {
    return type;
  }

  /**
   * @return the column namespace.
   */
  public Namespace namespace() {
    return namespace;
  }

  /**
   * @return the column namespace.
   */
  public Optional<String> headerKey() {
    return headerKey;
  }

  /**
   * @return index of the column <i>within the namespace</i>.
   */
  public int index() {
    return index;
  }

  public boolean canImplicitlyCast(final SqlType toType) {
    if (type instanceof SqlDecimal && toType instanceof SqlDecimal) {
      return DecimalUtil.canImplicitlyCast((SqlDecimal)type, (SqlDecimal)toType);
    }

    return type.equals(toType);
  }

  public boolean equalsIgnoreType(final Column that) {
    return Objects.equals(index, that.index)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(name, that.name)
        && Objects.equals(headerKey, that.headerKey);
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
    return equalsIgnoreType(that) && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, namespace, name, type);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    final String fmtNs;
    if (namespace == Namespace.HEADERS && headerKey.isPresent()) {
      fmtNs = " HEADER('" + headerKey.get() + "')";
    } else if (namespace == Namespace.VALUE) {
      fmtNs = "";
    } else {
      fmtNs = " " + namespace;
    }

    final String fmtType = type.toString(formatOptions);

    return name.toString(formatOptions) + " " + fmtType + fmtNs;
  }
}
