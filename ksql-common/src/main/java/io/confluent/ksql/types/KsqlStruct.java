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

package io.confluent.ksql.types;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.FieldName;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Instance of {@link io.confluent.ksql.schema.ksql.types.SqlStruct}.
 */
@Immutable
public final class KsqlStruct {

  private final SqlStruct schema;
  private final ImmutableList<Optional<?>> values;

  public static Builder builder(final SqlStruct schema) {
    return new Builder(schema);
  }

  private KsqlStruct(
      final SqlStruct schema,
      final List<Optional<?>> values
  ) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.values = ImmutableList.copyOf(Objects.requireNonNull(values, "values"));
  }

  public SqlStruct schema() {
    return this.schema;
  }

  public List<Optional<?>> values() {
    return values;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlStruct that = (KsqlStruct) o;
    return Objects.equals(schema, that.schema)
        && Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, values);
  }

  @Override
  public String toString() {
    return "KsqlStruct{"
        + "values=" + values
        + ", schema=" + schema
        + '}';
  }

  private static FieldInfo getField(final FieldName name, final SqlStruct schema) {
    final List<Field> fields = schema.getFields();

    for (int idx = 0; idx < fields.size(); idx++) {
      final Field field = fields.get(idx);
      if (field.fieldName().equals(name)) {
        return new FieldInfo(idx, field);
      }
    }

    throw new KsqlException("Unknown field: " + name);
  }

  public static final class Builder {

    private final SqlStruct schema;
    private final List<Optional<?>> values;

    public Builder(final SqlStruct schema) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.values = new ArrayList<>(schema.getFields().size());
      schema.getFields().forEach(f -> values.add(Optional.empty()));
    }

    public Builder set(final FieldName field, final Optional<?> value) {
      final FieldInfo info = getField(field, schema);
      info.field.type().validateValue(value.orElse(null));
      values.set(info.index, value);
      return this;
    }

    public Builder set(final String field, final Object value) {
      return set(FieldName.of(field), Optional.ofNullable(value));
    }

    public KsqlStruct build() {
      return new KsqlStruct(schema, values);
    }
  }

  private static final class FieldInfo {

    final int index;
    final Field field;

    private FieldInfo(final int index, final Field field) {
      this.index = index;
      this.field = Objects.requireNonNull(field, "field");
    }
  }
}
