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
import io.confluent.ksql.schema.ksql.SqlType;
import java.util.List;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public final class EntityUtil {

  private EntityUtil() {
  }

  public static List<FieldInfo> buildSourceSchemaEntity(final LogicalSchema schema) {
    return SchemaWalker.visit(schema.valueSchema(), new Converter())
        .getFields()
        .orElseThrow(() -> new RuntimeException("Root schema should contain fields"));
  }

  private static final class Converter implements SchemaWalker.Visitor<SchemaInfo, FieldInfo> {

    public SchemaInfo visitSchema(final Schema schema) {
      throw new IllegalArgumentException("Invalid type in schema: " + schema.type());
    }

    public SchemaInfo visitBoolean(final Schema schema) {
      return primitive(SqlType.BOOLEAN);
    }

    public SchemaInfo visitInt32(final Schema schema) {
      return primitive(SqlType.INTEGER);
    }

    public SchemaInfo visitInt64(final Schema schema) {
      return primitive(SqlType.BIGINT);
    }

    public SchemaInfo visitFloat64(final Schema schema) {
      return primitive(SqlType.DOUBLE);
    }

    public SchemaInfo visitString(final Schema schema) {
      return primitive(SqlType.STRING);
    }

    public SchemaInfo visitArray(final Schema schema, final SchemaInfo element) {
      return new SchemaInfo(SqlType.ARRAY, null, element);
    }

    public SchemaInfo visitMap(final Schema schema, final SchemaInfo key, final SchemaInfo value) {
      return new SchemaInfo(SqlType.MAP, null, value);
    }

    public SchemaInfo visitStruct(final Schema schema, final List<? extends FieldInfo> fields) {
      return new SchemaInfo(SqlType.STRUCT, fields, null);
    }

    public FieldInfo visitField(final Field field, final SchemaInfo type) {
      return new FieldInfo(field.name(), type);
    }

    private static SchemaInfo primitive(final SqlType type) {
      return new SchemaInfo(type, null, null);
    }
  }
}
