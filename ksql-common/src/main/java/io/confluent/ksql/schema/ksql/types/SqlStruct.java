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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Immutable
public final class SqlStruct extends SqlType {

  private final ImmutableList<Field> fields;

  public static Builder builder() {
    return new Builder();
  }

  private SqlStruct(final List<Field> fields) {
    super(SqlBaseType.STRUCT);
    this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields"));
  }

  public List<Field> getFields() {
    return fields;
  }

  @Override
  public boolean supportsCast() {
    return false;
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
    return fields.stream()
        .map(f -> f.toString(formatOptions))
        .collect(Collectors.joining(", ", "STRUCT<", ">"));
  }

  public static final class Builder {

    private final List<Field> fields = new ArrayList<>();

    public Builder field(final String fieldName, final SqlType fieldType) {
      return field(Field.of(fieldName, fieldType));
    }

    public Builder field(final Field field) {
      throwOnDuplicateFieldName(field);
      fields.add(field);
      return this;
    }

    public Builder fields(final Iterable<? extends Field> fields) {
      fields.forEach(this::field);
      return this;
    }

    public SqlStruct build() {
      if (fields.isEmpty()) {
        throw new KsqlException("STRUCT type must define fields");
      }
      return new SqlStruct(fields);
    }

    private void throwOnDuplicateFieldName(final Field other) {
      fields.stream()
          .filter(f -> f.fullName().equals(other.fullName()))
          .findAny()
          .ifPresent(duplicate -> {
            throw new KsqlException("Duplicate field names found in STRUCT: "
                + "'" + duplicate + "' and '" + other + "'");
          });
    }
  }
}
