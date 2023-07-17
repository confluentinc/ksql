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

package io.confluent.ksql.schema.ksql.types;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.schema.utils.DataException;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Immutable
public final class SqlStruct extends SqlType {

  private static final String PREFIX = "STRUCT<";
  private static final String POSTFIX = ">";
  private static final String EMPTY_STRUCT = PREFIX + " " + POSTFIX;

  private final ImmutableList<Field> fields;
  private final ImmutableMap<String, Field> byName;

  public static Builder builder() {
    return new Builder();
  }

  private SqlStruct(final List<Field> fields, final Map<String, Field> byName) {
    super(SqlBaseType.STRUCT);
    this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields"));
    this.byName = ImmutableMap.copyOf(requireNonNull(byName, "byName"));
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "fields is ImmutableList")
  public List<Field> fields() {
    return fields;
  }

  public Optional<Field> field(final String name) {
    return Optional.ofNullable(byName.get(name));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SqlStruct struct = (SqlStruct) o;
    return fields.equals(struct.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  @Override
  public String toString(final FormatOptions formatOptions) {
    if (fields.isEmpty()) {
      return EMPTY_STRUCT;
    }

    return fields.stream()
        .map(f -> f.toString(formatOptions))
        .collect(Collectors.joining(", ", PREFIX, POSTFIX));
  }

  public static final class Builder {

    private final List<Field> fields = new ArrayList<>();
    private final Map<String, Field> byName = new HashMap<>();

    public Builder field(final String fieldName, final SqlType fieldType) {
      final Field field = new Field(fieldName, fieldType, fields.size());
      if (byName.putIfAbsent(field.name(), field) != null) {
        throw new DataException("Duplicate field names found in STRUCT: "
            + "'" + byName.get(field.name()) + "' and '" + field + "'");
      }

      fields.add(field);
      return this;
    }

    public Builder field(final Field field) {
      field(field.name(), field.type());
      return this;
    }

    public Builder fields(final Iterable<? extends Field> fields) {
      fields.forEach(this::field);
      return this;
    }

    public SqlStruct build() {
      return new SqlStruct(fields, byName);
    }
  }

  @Immutable
  public static final class Field {

    private final String name;
    private final SqlType type;
    private final int index;

    @VisibleForTesting
    Field(final String name, final SqlType type, final int index) {
      this.name = requireNonNull(name, "name");
      this.type = requireNonNull(type, "type");
      this.index = index;

      if (!name.trim().equals(name)) {
        throw new IllegalArgumentException("name is not trimmed: '" + name + "'");
      }

      if (name.isEmpty()) {
        throw new IllegalArgumentException("name is empty");
      }

      if (index < 0) {
        throw new IllegalArgumentException("negative index: " + index);
      }
    }

    public SqlType type() {
      return type;
    }

    public String name() {
      return name;
    }

    /**
     * @return The index of the field within its parent struct.
     */
    public int index() {
      return index;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Field that = (Field) o;
      return index == that.index
          && Objects.equals(name, that.name)
          && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(index, name, type);
    }

    @Override
    public String toString() {
      return toString(FormatOptions.none());
    }

    public String toString(final FormatOptions formatOptions) {
      return formatOptions.escape(name) + " " + type.toString(formatOptions);
    }
  }
}
