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

import avro.shaded.com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.SchemaInfo;
import io.confluent.ksql.schema.SqlType;
import io.confluent.ksql.schema.ksql.KsqlSchema;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

public final class EntityUtil {
  private static final Map<Schema.Type, SqlType>
      SCHEMA_TYPE_TO_SCHEMA_INFO_TYPE =
      ImmutableMap.<Schema.Type, SqlType>builder()
          .put(Schema.Type.INT32, SqlType.INTEGER)
          .put(Schema.Type.INT64, SqlType.BIGINT)
          .put(Schema.Type.FLOAT32, SqlType.DOUBLE)
          .put(Schema.Type.FLOAT64, SqlType.DOUBLE)
          .put(Schema.Type.BOOLEAN, SqlType.BOOLEAN)
          .put(Schema.Type.STRING, SqlType.STRING)
          .put(Schema.Type.ARRAY, SqlType.ARRAY)
          .put(Schema.Type.MAP, SqlType.MAP)
          .put(Schema.Type.STRUCT, SqlType.STRUCT)
          .build();

  private EntityUtil() {
  }

  public static List<FieldInfo> buildSourceSchemaEntity(final KsqlSchema schema) {
    return buildSchemaEntity(schema.getSchema()).getFields()
        .orElseThrow(() -> new RuntimeException("Root schema should contain fields"));
  }

  private static SchemaInfo buildSchemaEntity(final Schema schema) {
    final SqlType sqlType = getSqlType(schema);

    switch (schema.type()) {
      case ARRAY:
      case MAP:
        return new SchemaInfo(
            sqlType,
            null,
            buildSchemaEntity(schema.valueSchema())
        );
      case STRUCT:
        return new SchemaInfo(
            sqlType,
            schema.fields()
                .stream()
                .map(
                    f -> new FieldInfo(f.name(), buildSchemaEntity(f.schema())))
                .collect(Collectors.toList()),
            null
        );
      default:
        return new SchemaInfo(sqlType, null, null);
    }
  }

  private static SqlType getSqlType(final Schema schema) {
    final SqlType type = SCHEMA_TYPE_TO_SCHEMA_INFO_TYPE.get(schema.type());
    if (type == null) {
      throw new RuntimeException(String.format("Invalid type in schema: %s.",
          schema.type().getName()));
    }

    return type;
  }
}
