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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.schema.ksql.types.Field;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.utils.DataException;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

/**
 * Instance of {@link io.confluent.ksql.schema.ksql.types.SqlStruct}.
 */
@EffectivelyImmutable
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

  public void forEach(final BiConsumer<? super Field, ? super Optional<?>> consumer) {
    for (int idx = 0; idx < values.size(); idx++) {
      final Field field = schema.fields().get(idx);
      final Optional<?> value = values.get(idx);
      consumer.accept(field, value);
    }
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

  private static FieldInfo getField(final String name, final SqlStruct schema) {
    final List<Field> fields = schema.fields();

    for (int idx = 0; idx < fields.size(); idx++) {
      final Field field = fields.get(idx);
      if (field.name().equals(name)) {
        return new FieldInfo(idx, field);
      }
    }

    throw new DataException("Unknown field: " + name);
  }

  public static final class Builder {

    private final SqlStruct schema;
    private final List<Optional<?>> values;

    public Builder(final SqlStruct schema) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.values = new ArrayList<>(schema.fields().size());
      schema.fields().forEach(f -> values.add(Optional.empty()));
    }

    public Builder set(final String field, final Object value) {
      return set(field, Optional.of(value));
    }

    public Builder set(final String field, final Optional<?> value) {
      final FieldInfo info = getField(field, schema);
      validateValue(info.field.type(), value.orElse(null));
      values.set(info.index, value);
      return this;
    }

    public KsqlStruct build() {
      return new KsqlStruct(schema, values);
    }

    @VisibleForTesting
    public void validateValue(final SqlType type, final Object value) {
      if (value == null) {
        return;
      }

      if (type instanceof SqlDecimal) {
        validateDecimal((SqlDecimal) type, value);
      } else if (type instanceof SqlArray) {
        validateArray((SqlArray)type, value);
      } else if (type instanceof SqlMap) {
        validateMap((SqlMap)type, value);
      } else if (type instanceof SqlPrimitiveType) {
        validatePrimitive((SqlPrimitiveType)type, value);
      } else if (type instanceof SqlStruct) {
        validateStruct((SqlStruct)type, value);
      }
    }

    private void validateDecimal(final SqlDecimal decimalType, final Object value) {
      if (!(value instanceof BigDecimal)) {
        final SqlBaseType sqlBaseType = SchemaConverters.javaToSqlConverter()
            .toSqlType(value.getClass());

        throw new DataException("Expected DECIMAL, got " + sqlBaseType);
      }

      final BigDecimal decimal = (BigDecimal) value;
      if (decimal.precision() != decimalType.getPrecision()) {
        throw new DataException("Expected " + decimalType + ", got precision "
                                    + decimal.precision());
      }

      if (decimal.scale() != decimalType.getScale()) {
        throw new DataException("Expected " + decimalType + ", got scale " + decimal.scale());
      }
    }

    private void validateArray(final SqlArray arrayType, final Object value) {
      if (!(value instanceof List)) {
        final SqlBaseType sqlBaseType = SchemaConverters.javaToSqlConverter()
            .toSqlType(value.getClass());

        throw new DataException("Expected ARRAY, got " + sqlBaseType);
      }

      final List<?> array = (List<?>) value;

      IntStream.range(0, array.size()).forEach(idx -> {
        try {
          final Object element = array.get(idx);
          validateValue(arrayType.getItemType(), element);
        } catch (final DataException e) {
          throw new DataException("ARRAY element " + (idx + 1) + ": " + e.getMessage(), e);
        }
      });
    }

    private void validateMap(final SqlMap mapType, final Object value) {
      if (!(value instanceof Map)) {
        final SqlBaseType sqlBaseType = SchemaConverters.javaToSqlConverter()
            .toSqlType(value.getClass());

        throw new DataException("Expected MAP, got " + sqlBaseType);
      }

      final Map<?, ?> map = (Map<?, ?>) value;
      map.forEach((k, v) -> {
        try {
          validateValue(SqlMap.KEY_TYPE, k);
        } catch (final DataException e) {
          throw new DataException("MAP key: " + e.getMessage(), e);
        }

        try {
          validateValue(mapType.getValueType(), v);
        } catch (final DataException e) {
          throw new DataException("MAP value for key '" + k + "': " + e.getMessage(), e);
        }
      });
    }

    private void validatePrimitive(final SqlPrimitiveType primitiveType, final Object value) {
      final SqlBaseType actualType = SchemaConverters.javaToSqlConverter()
          .toSqlType(value.getClass());

      if (!primitiveType.baseType().equals(actualType)) {
        throw new DataException("Expected " + primitiveType.baseType() + ", got " + actualType);
      }
    }

    private void validateStruct(final SqlStruct structType, final Object value) {
      if (!(value instanceof KsqlStruct)) {
        final SqlBaseType sqlBaseType = SchemaConverters.javaToSqlConverter()
            .toSqlType(value.getClass());

        throw new DataException("Expected STRUCT, got " + sqlBaseType);
      }

      final KsqlStruct struct = (KsqlStruct)value;
      if (!struct.schema().equals(structType)) {
        throw new DataException("Expected " + structType + ", got " + struct.schema());
      }
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
