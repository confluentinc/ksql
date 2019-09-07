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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Optional;

/**
 * A named field within KSQL schema types.
 */
@Immutable
public final class Field {
  private static final String SOURCE = "source";
  private static final String NAME = "name";
  private static final String TYPE = "type";

  @JsonProperty(SOURCE)
  private final Optional<String> source;
  @JsonProperty(NAME)
  private final String name;
  @JsonProperty(TYPE)
  private final SqlType type;

  private transient final String fullName;

  /**
   * @param name the name of the field.
   * @param type the type of the field.
   * @return the immutable field.
   */
  public static Field of(final String name, final SqlType type) {
    return new Field(Optional.empty(), name, type);
  }

  /**
   * @param source the name of the source of the field.
   * @param name the name of the field.
   * @param type the type of the field.
   * @return the immutable field.
   */
  public static Field of(final String source, final String name, final SqlType type) {
    return new Field(Optional.of(source), name, type);
  }

  /**
   * @param source the name of the source of the field.
   * @param name the name of the field.
   * @param type the type of the field.
   * @return the immutable field.
   */
  public static Field of(final Optional<String> source, final String name, final SqlType type) {
    return new Field(source, name, type);
  }

  @JsonCreator
  private Field(
      @JsonProperty(SOURCE) final Optional<String> source,
      @JsonProperty(NAME) final String name,
      @JsonProperty(TYPE) final SqlType type) {
    this.source = Objects.requireNonNull(source, "source");
    this.name = Objects.requireNonNull(name, "name");
    this.type = Objects.requireNonNull(type, "type");
    this.fullName = source
        .map(s -> SchemaUtil.buildAliasedFieldName(s, name))
        .orElse(name);

    if (!name.trim().equals(name)) {
      throw new IllegalArgumentException("name is not trimmed: '" + name + "'");
    }

    if (name.isEmpty()) {
      throw new IllegalArgumentException("name is empty");
    }
  }

  /**
   * @return the name of the source of the field, where known.
   */
  public Optional<String> source() {
    return source;
  }

  /**
   * @return the fully qualified field name.
   */
  public String fullName() {
    return fullName;
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
  public Field withSource(final String source) {
    return new Field(Optional.of(source), name, type);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Field field = (Field) o;
    return Objects.equals(fullName, field.fullName)
        && Objects.equals(type, field.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fullName, type);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    final Optional<String> base = source.map(val -> escape(val, formatOptions));
    final String escaped = escape(name, formatOptions);
    final String field = base.isPresent() ? base.get() + "." + escaped : escaped;
    return field + " " + type.toString(formatOptions);
  }

  private static String escape(final String string, final FormatOptions formatOptions) {
    return formatOptions.isReservedWord(string) ? "`" + string + "`" : string;
  }
}
