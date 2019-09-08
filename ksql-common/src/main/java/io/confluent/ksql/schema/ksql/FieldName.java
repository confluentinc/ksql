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
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable POJO for storing a {@link Field}'s name
 */
@Immutable
public final class FieldName {

  private final Optional<String> source;
  private final String name;
  private final String fullName;

  public static FieldName of(final String name) {
    return of(Optional.empty(), name);
  }

  public static FieldName of(final String source, final String name) {
    return of(Optional.of(source), name);
  }

  public static FieldName of(final Optional<String> source, final String name) {
    return new FieldName(source, name);
  }

  private FieldName(final Optional<String> source, final String fullName) {
    this.source = Objects.requireNonNull(source, "source");
    this.name = Objects.requireNonNull(fullName, "name");
    this.fullName = source
        .map(s -> SchemaUtil.buildAliasedFieldName(s, name))
        .orElse(name);

    this.source.ifPresent(src -> {
      if (!src.trim().equals(src)) {
        throw new IllegalArgumentException("source is not trimmed: '" + src + "'");
      }

      if (src.isEmpty()) {
        throw new IllegalArgumentException("source is empty");
      }

    });

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
   * @return the name of the field.
   */
  public String name() {
    return name;
  }

  /**
   * @return the fully qualified field name.
   */
  public String fullName() {
    return fullName;
  }

  public FieldName withSource(final String source) {
    return new FieldName(Optional.of(source), name);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final FieldName fieldName = (FieldName) o;
    return Objects.equals(fullName, fieldName.fullName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fullName);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    final Optional<String> base = source.map(val -> escape(val, formatOptions));
    final String escaped = escape(name, formatOptions);
    return base.map(s -> s + "." + escaped).orElse(escaped);
  }

  private static String escape(final String string, final FormatOptions formatOptions) {
    return formatOptions.isReservedWord(string) ? "`" + string + "`" : string;
  }
}
