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
import io.confluent.ksql.schema.connect.SchemaWalker;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public final class EntityUtil {

  private EntityUtil() {
  }

  public static List<FieldInfo> buildSourceSchemaEntity(
      final LogicalSchema schema,
      final boolean valueSchemaOnly
  ) {

    final List<FieldInfo> allFields = new ArrayList<>();
    if (!valueSchemaOnly) {
      allFields.addAll(getFields(schema.metaFields(), "implicit"));
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
        .map(field -> new FieldInfo(field.name(), getSchema(field.schema())))
        .collect(Collectors.toList());
  }

  private static SchemaInfo getSchema(final Schema schema) {
    return SchemaWalker.visit(schema, new Converter());
  }

  private static final class Converter implements SchemaWalker.Visitor<SchemaInfo, FieldInfo> {

    public SchemaInfo visitSchema(final Schema schema) {
      throw new IllegalArgumentException("Invalid type in schema: " + schema.type());
    }

    public SchemaInfo visitBoolean(final Schema schema) {
      return primitive(SqlBaseType.BOOLEAN);
    }

    public SchemaInfo visitInt32(final Schema schema) {
      return primitive(SqlBaseType.INTEGER);
    }

    public SchemaInfo visitInt64(final Schema schema) {
      return primitive(SqlBaseType.BIGINT);
    }

    public SchemaInfo visitFloat64(final Schema schema) {
      return primitive(SqlBaseType.DOUBLE);
    }

    public SchemaInfo visitString(final Schema schema) {
      return primitive(SqlBaseType.STRING);
    }

    public SchemaInfo visitArray(final Schema schema, final SchemaInfo element) {
      return new SchemaInfo(SqlBaseType.ARRAY, null, element);
    }

    public SchemaInfo visitMap(final Schema schema, final SchemaInfo key, final SchemaInfo value) {
      return new SchemaInfo(SqlBaseType.MAP, null, value);
    }

    public SchemaInfo visitStruct(final Schema schema, final List<? extends FieldInfo> fields) {
      return new SchemaInfo(SqlBaseType.STRUCT, fields, null);
    }

    public FieldInfo visitField(final Field field, final SchemaInfo type) {
      return new FieldInfo(field.name(), type);
    }

    private static SchemaInfo primitive(final SqlBaseType type) {
      return new SchemaInfo(type, null, null);
    }
  }
}
