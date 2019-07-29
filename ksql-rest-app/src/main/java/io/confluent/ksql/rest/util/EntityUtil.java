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
import io.confluent.ksql.rest.entity.SchemaInfo;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.SqlTypeWalker;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class EntityUtil {

  private EntityUtil() {
  }

  public static List<FieldInfo> buildSourceSchemaEntity(
      final LogicalSchema schema,
      final boolean valueSchemaOnly
  ) {

    final List<FieldInfo> allFields = new ArrayList<>();
    if (!valueSchemaOnly) {
      allFields.addAll(getFields(schema.metaFields(), "meta"));
      allFields.addAll(getFields(schema.keyFields(), "key"));
    }
    allFields.addAll(getFields(schema.valueFields(), "value"));

    return allFields;
  }

  private static List<FieldInfo> getFields(final List<Field> fields, final String type) {
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("Root schema should contain fields." + " type: " + type);
    }

    return fields.stream()
        .map(field -> SqlTypeWalker.visit(field, new Converter()))
        .collect(Collectors.toList());
  }

  private static SchemaInfo getSchema(final SqlType type) {
    return SqlTypeWalker.visit(type, new Converter());
  }

  private static final class Converter implements SqlTypeWalker.Visitor<SchemaInfo, FieldInfo> {

    public SchemaInfo visitType(final SqlType schema) {
      return new SchemaInfo(schema.baseType(), null, null);
    }

    public SchemaInfo visitArray(final SqlArray type, final SchemaInfo element) {
      return new SchemaInfo(SqlBaseType.ARRAY, null, element);
    }

    public SchemaInfo visitMap(final SqlMap type, final SchemaInfo value) {
      return new SchemaInfo(SqlBaseType.MAP, null, value);
    }

    public SchemaInfo visitStruct(final SqlStruct type, final List<? extends FieldInfo> fields) {
      return new SchemaInfo(SqlBaseType.STRUCT, fields, null);
    }

    public FieldInfo visitField(final Field field, final SchemaInfo type) {
      return new FieldInfo(field.fullName(), type);
    }
  }
}
