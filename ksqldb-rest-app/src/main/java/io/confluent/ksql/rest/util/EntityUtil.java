/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.FieldInfo.FieldType;
import io.confluent.ksql.rest.entity.SchemaInfo;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlTypeWalker;
import io.confluent.ksql.schema.ksql.types.Field;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public final class EntityUtil {

  private EntityUtil() {
  }

  public static List<FieldInfo> buildSourceSchemaEntity(final LogicalSchema schema) {
    final List<FieldInfo> allFields = schema.columns().stream()
        .map(EntityUtil::toFieldInfo)
        .collect(Collectors.toList());

    if (allFields.isEmpty()) {
      throw new IllegalArgumentException("Root schema should contain columns: " + schema);
    }

    return allFields;
  }

  public static SchemaInfo schemaInfo(final SqlType type) {
    return SqlTypeWalker.visit(type, new Converter(Optional.empty()));
  }

  private static FieldInfo toFieldInfo(final Column column) {
    return SqlTypeWalker.visit(
        Field.of(column.name().text(), column.type()), new Converter(Optional.of(column)));
  }

  private static final class Converter implements SqlTypeWalker.Visitor<SchemaInfo, FieldInfo> {

    private final Optional<Column> column;

    Converter(final Optional<Column> column) {
      this.column = Objects.requireNonNull(column, "column");
    }

    public SchemaInfo visitType(final SqlType schema) {
      return new SchemaInfo(schema.baseType(), null, null);
    }

    public SchemaInfo visitArray(final SqlArray type, final SchemaInfo element) {
      return new SchemaInfo(SqlBaseType.ARRAY, null, element);
    }

    public SchemaInfo visitMap(final SqlMap type, final SchemaInfo key, final SchemaInfo value) {
      return new SchemaInfo(SqlBaseType.MAP, null, value);
    }

    public SchemaInfo visitStruct(final SqlStruct type, final List<? extends FieldInfo> fields) {
      return new SchemaInfo(SqlBaseType.STRUCT, fields, null);
    }

    public FieldInfo visitField(final Field field, final SchemaInfo type) {
      final Optional<FieldType> fieldType = column
          .filter(col -> field.name().equals(col.name().text()))
          .flatMap(col -> toFieldType(col.namespace()));

      return new FieldInfo(field.name(), type, fieldType);
    }

    private static Optional<FieldType> toFieldType(final Column.Namespace ns) {
      return ns == Namespace.KEY
          ? Optional.of(FieldType.KEY)
          : Optional.empty();
    }
  }
}
