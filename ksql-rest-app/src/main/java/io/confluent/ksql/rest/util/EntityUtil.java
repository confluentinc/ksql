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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

public final class EntityUtil {
  private static final Map<Schema.Type, SchemaInfo.Type>
      SCHEMA_TYPE_TO_SCHEMA_INFO_TYPE =
      ImmutableMap.<Schema.Type, SchemaInfo.Type>builder()
          .put(Schema.Type.INT32, SchemaInfo.Type.INTEGER)
          .put(Schema.Type.INT64, SchemaInfo.Type.BIGINT)
          .put(Schema.Type.FLOAT32, SchemaInfo.Type.DOUBLE)
          .put(Schema.Type.FLOAT64, SchemaInfo.Type.DOUBLE)
          .put(Schema.Type.BOOLEAN, SchemaInfo.Type.BOOLEAN)
          .put(Schema.Type.STRING, SchemaInfo.Type.STRING)
          .put(Schema.Type.ARRAY, SchemaInfo.Type.ARRAY)
          .put(Schema.Type.MAP, SchemaInfo.Type.MAP)
          .put(Schema.Type.STRUCT, SchemaInfo.Type.STRUCT)
          .build();

  private EntityUtil() {
  }

  public static List<FieldInfo> buildSourceSchemaEntity(final Schema schema) {
    return buildSchemaEntity(schema).getFields()
        .orElseThrow(() -> new RuntimeException("Root schema should contain fields"));
  }

  private static SchemaInfo buildSchemaEntity(final Schema schema) {
    switch (schema.type()) {
      case ARRAY:
      case MAP:
        return new SchemaInfo(
            getSchemaTypeString(schema),
            null,
            buildSchemaEntity(schema.valueSchema())
        );
      case STRUCT:
        return new SchemaInfo(
            getSchemaTypeString(schema),
            schema.fields()
                .stream()
                .map(
                    f -> new FieldInfo(f.name(), buildSchemaEntity(f.schema())))
                .collect(Collectors.toList()),
            null
        );
      default:
        return new SchemaInfo(getSchemaTypeString(schema), null, null);
    }
  }

  private static SchemaInfo.Type getSchemaTypeString(final Schema schema) {
    final SchemaInfo.Type type = SCHEMA_TYPE_TO_SCHEMA_INFO_TYPE.get(schema.type());
    if (type == null) {
      throw new RuntimeException(String.format("Invalid type in schema: %s.",
          schema.type().getName()));
    }

    return type;
  }
}
