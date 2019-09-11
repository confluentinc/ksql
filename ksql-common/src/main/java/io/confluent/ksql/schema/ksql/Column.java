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

import static io.confluent.ksql.util.Identifiers.ensureTrimmed;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.Identifiers;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Optional;

/**
 * A named field within KSQL schema types.
 */
@Immutable
public final class Column {

  private final Optional<String> source;
  private final String name;
  private final SqlType type;

  /**
   * @param name the name of the field.
   * @param type the type of the field.
   * @return the immutable field.
   */
  public static Column of(final String name, final SqlType type) {
    return new Column(Optional.empty(), name, type);
  }

  /**
   * @param source the name of the source of the field.
   * @param name the name of the field.
   * @param type the type of the field.
   * @return the immutable field.
   */
  public static Column of(final String source, final String name, final SqlType type) {
    return new Column(Optional.of(source), name, type);
  }

  /**
   * @param source the name of the source of the field.
   * @param name the name of the field.
   * @param type the type of the field.
   * @return the immutable field.
   */
  public static Column of(final Optional<String> source, final String name, final SqlType type) {
    return new Column(source, name, type);
  }

  private Column(final Optional<String> source, final String name, final SqlType type) {
    this.source = Objects.requireNonNull(source, "source").map(src -> ensureTrimmed(src, "source"));
    this.name = ensureTrimmed(Objects.requireNonNull(name, "name"), "name");
    this.type = Objects.requireNonNull(type, "type");
  }

  /**
   * @return the fully qualified field name.
   */
  public String fullName() {
    return source.map(alias -> SchemaUtil.buildAliasedFieldName(alias, name)).orElse(name);
  }

  /**
   * @return the source of the Column
   */
  public Optional<String> source() {
    return source;
  }

  /**
   * @return the name of the field, without any source / alias.
   */
  public String name() {
    return name;
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
  public Column withSource(final String source) {
    return new Column(Optional.of(source), name, type);
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
    return Objects.equals(source, that.source)
        && Objects.equals(name, that.name)
        && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, name, type);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    final String fmtName = Identifiers.escape(name, formatOptions);
    final String fmtType = type.toString(formatOptions);
    final String fmtSource = source.map(s -> Identifiers.escape(s, formatOptions) + ".").orElse("");

    return fmtSource + fmtName + " " + fmtType;
  }
}
